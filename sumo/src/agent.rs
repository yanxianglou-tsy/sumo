use std::collections::BTreeMap;

use crate::service::{RunningService, ServiceName};

const SUMO_AGENT_ENV_SERVICE_NAME: &str = "SUMO_AGENT_ENV_SERVICE_NAME";
const SUMO_AGENT_ENV_START_TIME: &str = "SUMO_AGENT_ENV_START_TIME";

const INHERITABLE_ENVIRONMENT_VARIABLES: &[&str] = &[
    "LANG", "LANGUAGE", "USER", "LOGNAME", "HOME", "PATH", "SHELL", "TERM",
];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ServiceUpdate {
    AllServicesReport {
        services: BTreeMap<ServiceName, Vec<RunningService>>,
    },
    PollingServicesError {
        err: String,
    },
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    #[clap(display_order = 10, about = "Start the agent daemon on current server")]
    Daemon(daemon::Args),
}

pub fn run_command(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Daemon(args) => daemon::run(args),
    }
}

pub mod daemon {
    use std::{
        collections::BTreeMap,
        ffi::{CString, OsString},
        net::SocketAddrV4,
        os::fd::IntoRawFd,
        path::{Path, PathBuf},
        pin::Pin,
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use anyhow::Context;
    use futures::Stream;
    use tokio::{signal::unix::SignalKind, sync::mpsc::UnboundedSender};
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tracing::{debug, error, info, warn};

    use super::{
        RunningService, ServiceUpdate, INHERITABLE_ENVIRONMENT_VARIABLES,
        SUMO_AGENT_ENV_SERVICE_NAME, SUMO_AGENT_ENV_START_TIME,
    };
    use crate::{
        service::{ServiceDefinition, ServiceName},
        sumo_protocol::{
            agent_rpc_server::{AgentRpc, AgentRpcServer},
            AgentStartServiceRequest, LoginRequest, ServiceUpdateWrapper, StartServiceResponse,
            StopServiceRequest, StopServiceResponse,
        },
    };

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct AgentConfig {
        base_directory: Option<PathBuf>,
        rpc_server_address: SocketAddrV4,
        log_level: String,
        running_services_polling_interval_in_seconds: u64,
    }

    struct Agent {
        running_services: BTreeMap<ServiceName, Vec<RunningService>>,
        update_subscribers:
            Vec<UnboundedSender<tonic::Result<ServiceUpdateWrapper, tonic::Status>>>,
        shutdown_requested: bool,
    }

    impl Agent {
        pub fn new(running_services: BTreeMap<ServiceName, Vec<RunningService>>) -> Self {
            Self {
                running_services,
                update_subscribers: Vec::new(),
                shutdown_requested: false,
            }
        }
    }

    fn start_service(def: ServiceDefinition) -> anyhow::Result<RunningService> {
        use nix::unistd::{fork, ForkResult};

        match unsafe { fork() } {
            Err(err) => {
                anyhow::bail!("Failed to fork in order to start service {def:?}, err: {err}");
            }
            Ok(ForkResult::Parent { child }) => {
                info!("Forked child process {child} for service {}", def.name);
                let running_service = RunningService {
                    service_name: def.name,
                    pid: child.as_raw(),
                    start_time: chrono::Local::now().into(),
                };
                // We record the child service pid here as a running service. If the process forks
                // again (e.g. to daemonize) or fails to exec, it will be picked up by subsequent
                // polling.
                Ok(running_service)
            }
            Ok(ForkResult::Child) => {
                nix::unistd::chdir(&def.working_directory).unwrap();

                fn redirect_fd_to_file(fd: i32, path: &Path) {
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .write(true)
                        .open(path)
                        .unwrap()
                        .into_raw_fd();
                    nix::unistd::dup2(file, fd).unwrap();
                }
                redirect_fd_to_file(libc::STDOUT_FILENO, &def.redirect_stdout_to);
                redirect_fd_to_file(libc::STDERR_FILENO, &def.redirect_stderr_to);

                let exe = def.executable.to_str().unwrap();
                let args: Vec<_> = {
                    std::iter::once(exe.to_owned())
                        .chain(def.args.into_iter())
                        .map(|arg| CString::new(arg.as_bytes()).unwrap())
                        .collect()
                };
                let envs: Vec<_> = std::env::vars_os()
                    .filter_map(|(key, val)| match (key.to_str(), val.to_str()) {
                        (None, _) | (_, None) => None,
                        (Some(key), Some(val)) => {
                            match INHERITABLE_ENVIRONMENT_VARIABLES.contains(&key) {
                                false => None,
                                true => CString::new(format!("{}={}", key, val)).ok(),
                            }
                        }
                    })
                    .chain(
                        def.envs
                            .into_iter()
                            .filter_map(|(key, val)| CString::new(format!("{}={}", key, val)).ok()),
                    )
                    .chain({
                        let service_name = format!("{}={}", SUMO_AGENT_ENV_SERVICE_NAME, def.name);
                        let start_time = {
                            let now = chrono::Local::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, false);
                            format!("{}={}", SUMO_AGENT_ENV_START_TIME, now)
                        };
                        [service_name, start_time]
                            .into_iter()
                            .map(|x| CString::new(x).unwrap())
                    })
                    .collect();
                nix::unistd::execve(&CString::new(exe).unwrap(), &args, &envs).unwrap_or_else(
                    |err| {
                        panic!("Failed to execve exe={exe:?}, args={args:?}, envs={envs:?}: {err}");
                    },
                );
                unreachable!("Never here");
            }
        }
    }

    #[tonic::async_trait]
    impl AgentRpc for Arc<Mutex<Agent>> {
        async fn start_service(
            &self,
            request: tonic::Request<AgentStartServiceRequest>,
        ) -> Result<tonic::Response<StartServiceResponse>, tonic::Status> {
            info!("Got request to start service {:?}", request);
            let def: ServiceDefinition = {
                let json = request.into_inner().service_definition_json;
                match serde_json::from_str(&json) {
                    Ok(def) => def,
                    Err(err) => {
                        return Err(tonic::Status::aborted(format!(
                            "Invalid service definition {json}: {err}"
                        )));
                    }
                }
            };
            let service_name = def.name.clone();
            let mut agent = self.lock().unwrap();
            if agent.running_services.contains_key(&service_name) {
                return Result::Err(tonic::Status::already_exists(format!(
                    "Service {} is already running.",
                    def.name
                )));
            }
            match start_service(def) {
                Ok(service) => {
                    // We record the running service here so if another [start_service] RPC request
                    // comes in, it will be rejected. Otherwise there is a small race condition
                    // window between a) service is started here, and b) service is recognized in
                    // next polling.  If the service crashed/stopped later on, it will be removed
                    // by a subsequent polling and one can [start_service] again.
                    agent.running_services.insert(service_name, vec![service]);
                    Ok(tonic::Response::new(StartServiceResponse {}))
                }
                Err(err) => Err(tonic::Status::aborted(format!(
                    "Service {service_name} start failed, err: {err}"
                ))),
            }
        }

        async fn stop_service(
            &self,
            request: tonic::Request<StopServiceRequest>,
        ) -> Result<tonic::Response<StopServiceResponse>, tonic::Status> {
            info!("Got request to stop service {:?}", request);

            let service_name = {
                let name = &request.into_inner().service_name;
                match ServiceName::from_str(name) {
                    Ok(name) => name,
                    Err(err) => {
                        return Err(tonic::Status::invalid_argument(format!(
                            "Ill-formatted service name {name}: {err}"
                        )))
                    }
                }
            };
            let running_services = {
                let mut agent = self.lock().unwrap();
                agent
                    .running_services
                    .remove(&service_name)
                    .unwrap_or_default()
            };
            // Do nothing if the service is not running and do not return an error.
            for running_service in running_services {
                // It's possible that there is a small race condition that a process has already
                // existed but the `running_services` is not updated yet. We just ignore the error
                // when sending a signal.
                match nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(running_service.pid),
                    Some(nix::sys::signal::Signal::SIGTERM),
                ) {
                    Ok(()) => {}
                    Err(err) => {
                        error!(
                            "Failed to send SIGTERM to pid {}: {err}",
                            running_service.pid
                        );
                    }
                };
            }
            Ok(tonic::Response::new(StopServiceResponse {}))
        }

        type LoginStream =
            Pin<Box<dyn Stream<Item = Result<ServiceUpdateWrapper, tonic::Status>> + Send>>;

        async fn login(
            &self,
            request: tonic::Request<LoginRequest>,
        ) -> Result<tonic::Response<Self::LoginStream>, tonic::Status> {
            let mut agent = self.lock().unwrap();
            info!(
                "Got login request from {}.",
                request
                    .remote_addr()
                    .expect("Remote address not found for Login request."),
            );
            let (updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel();
            agent.update_subscribers.push(updates_tx);
            let output_stream = UnboundedReceiverStream::new(updates_rx);
            Ok(tonic::Response::new(Box::pin(output_stream)))
        }
    }

    #[derive(Debug, clap::Args)]
    pub struct Args {
        #[clap(long)]
        pub config_file: PathBuf,
    }

    pub fn run(args: Args) -> anyhow::Result<()> {
        let config: AgentConfig = {
            let config =
                std::fs::read_to_string(args.config_file).context("Loading config file")?;
            serde_json::from_str(&config).context("Parsing config file")?
        };
        // The base directory defaults to ~/app/sumo/
        let base_directory = match &config.base_directory {
            Some(dir) => dir.to_path_buf(),
            None => {
                let user = {
                    let uid = nix::unistd::getuid();
                    nix::unistd::User::from_uid(uid)?
                        .expect("Failed to get user name")
                        .name
                };
                PathBuf::from_str(&format!("/home/{user}/app/sumo")).unwrap()
            }
        };
        std::fs::create_dir_all(&base_directory).context("Creating base directory")?;

        daemonize::Daemonize::new()
            .working_directory(&base_directory)
            .pid_file(base_directory.join("agent.pid"))
            .chown_pid_file(true)
            .start()
            .context("Failed to daemonize agent process")?;

        run_impl(base_directory, config)
    }

    #[tokio::main]
    async fn run_impl(base_directory: PathBuf, config: AgentConfig) -> anyhow::Result<()> {
        let logfile = tracing_appender::rolling::daily(&base_directory, "agent-log");
        let log_level = tracing::Level::from_str(&config.log_level).context("Invalid log level")?;
        tracing_subscriber::fmt()
            .with_writer(logfile)
            .with_max_level(log_level)
            .with_ansi(false)
            .init();

        info!(
            "Starting sumo agent daemon with config {config:?} and base directory {}",
            base_directory.display()
        );

        // Make this process the subreaper of all descendant processes
        let ret = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) };
        if ret != 0 {
            panic!("Failed to set the agent process as subreaper: ret={}", ret);
        }

        let agent = {
            let existing_services = poll_running_sumo_processes()?;
            if !existing_services.is_empty() {
                warn!("Found already running services: {:?}", existing_services);
            }
            Arc::new(Mutex::new(Agent::new(existing_services)))
        };

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let poll_running_services = {
            let agent = agent.clone();
            tokio::spawn(async move {
                let mut interval = {
                    let poll_interval = tokio::time::Duration::from_secs(
                        config.running_services_polling_interval_in_seconds,
                    );
                    tokio::time::interval(poll_interval)
                };
                loop {
                    interval.tick().await;
                    {
                        let agent = agent.lock().unwrap();
                        if agent.shutdown_requested {
                            return;
                        }
                    }
                    match poll_running_sumo_processes() {
                        Err(err) => {
                            error!("Failed to poll running services, err: {:?}", err);
                            let update = {
                                let update = ServiceUpdate::PollingServicesError {
                                    err: err.to_string(),
                                };
                                tonic::Result::Ok(ServiceUpdateWrapper {
                                    service_update_json: serde_json::to_string(&update).unwrap(),
                                })
                            };
                            let mut agent = agent.lock().unwrap();
                            agent
                                .update_subscribers
                                .retain_mut(|subscriber| subscriber.send(update.clone()).is_ok());
                        }
                        Ok(services) => {
                            info!(
                                "Polling running service - currently running services: {services:?}"
                            );
                            update_running_services(agent.clone(), services);
                        }
                    }
                }
            })
        };
        let rpc_server = {
            let agent = agent.clone();
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(AgentRpcServer::new(agent))
                    .serve_with_shutdown(config.rpc_server_address.into(), async move {
                        shutdown_rx.recv().await;
                        info!("Shutdown requested, shutting down RPC server.");
                    })
                    .await
                    .expect("Error running agent RPC server")
            })
        };
        info!(
            "Started RPC server listening on {}",
            config.rpc_server_address
        );

        // Handler for shutdown (SIGTERM / SIGINT) signals
        register_shutdown_on_signal(agent.clone(), shutdown_tx.clone(), SignalKind::terminate());
        register_shutdown_on_signal(agent.clone(), shutdown_tx.clone(), SignalKind::interrupt());

        // Handler for SIGCHLD signal
        wait_for_child_processes(agent.clone());

        let (poll_services, rpc_server) = tokio::join!(poll_running_services, rpc_server);
        poll_services.expect("Service updates polling exited with error");
        rpc_server.expect("RPC server didn't finish successfully");
        Ok(())
    }

    fn update_running_services(
        agent: Arc<Mutex<Agent>>,
        services: BTreeMap<ServiceName, Vec<RunningService>>,
    ) {
        let update = {
            let update = ServiceUpdate::AllServicesReport {
                services: services.clone(),
            };
            tonic::Result::Ok(ServiceUpdateWrapper {
                service_update_json: serde_json::to_string(&update).unwrap(),
            })
        };

        let mut agent = agent.lock().unwrap();
        agent.running_services = services;
        agent
            .update_subscribers
            .retain_mut(|subscriber| subscriber.send(update.clone()).is_ok());
    }

    // Collect dead child processes
    fn wait_for_child_processes(agent: Arc<Mutex<Agent>>) {
        tokio::spawn(async move {
            loop {
                {
                    let agent = agent.lock().unwrap();
                    if agent.shutdown_requested {
                        return;
                    }
                }
                // Polling wait (WNOHANG) instead of blocking wait because the blocking call
                // will block the runtime shutdown.
                let wait_status = tokio::task::spawn_blocking(|| {
                    nix::sys::wait::waitpid(None, Some(nix::sys::wait::WaitPidFlag::WNOHANG))
                })
                .await
                .expect("Failed to waitpid");
                match wait_status {
                    Err(nix::errno::Errno::ECHILD) => {
                        // Do nothing, no child processes
                    }
                    Err(err) => {
                        error!("Failed to waitpid, err: {err}");
                    }
                    Ok(nix::sys::wait::WaitStatus::StillAlive) => {
                        // Do nothing, child process(es) still running normally
                    }
                    Ok(wait_status) => {
                        debug!("Got child process wait status change: {:?}", wait_status);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    fn register_shutdown_on_signal(
        agent: Arc<Mutex<Agent>>,
        shutdown_tx: tokio::sync::mpsc::UnboundedSender<()>,
        signal_kind: SignalKind,
    ) {
        tokio::spawn({
            async move {
                let mut signals = tokio::signal::unix::signal(signal_kind).unwrap();
                signals.recv().await;
                warn!(
                    "Signal {} received, shutting down",
                    signal_kind.as_raw_value()
                );
                agent.lock().unwrap().shutdown_requested = true;
                shutdown_tx.send(()).unwrap_or(());
            }
        });
    }

    /// Poll all running processes to find out services by checking SUMO_AGENT_ENV_SERVICE_NAME
    /// environment variable.
    fn poll_running_sumo_processes() -> anyhow::Result<BTreeMap<ServiceName, Vec<RunningService>>> {
        let uid = nix::unistd::getuid().as_raw();
        let processes = procfs::process::all_processes()?.filter_map(|process| match process {
            Err(_) => None,
            Ok(process) => {
                if let Ok(process_uid) = process.uid() {
                    if process_uid == uid {
                        return Some(process);
                    }
                }
                None
            }
        });
        let service_name_env = OsString::from_str(SUMO_AGENT_ENV_SERVICE_NAME).unwrap();
        let start_time_env = OsString::from_str(SUMO_AGENT_ENV_START_TIME).unwrap();
        let mut found_services = BTreeMap::default();
        for process in processes {
            // It's possible for some processes to be owned by current user but the environ (and
            // other information) is not permissioned to current user, for example when a process is
            // started with setuid.  We don't expect this to happen for services managed by sumo so
            // just skip this case here.
            if let Ok(environ) = process.environ() {
                if let Some(service_name) = environ.get(&service_name_env) {
                    let pid = process.pid();
                    let service_name = ServiceName::from_str(
                        service_name
                            .to_str()
                            .context(format!("Invalid service name {service_name:?}"))?,
                    )
                    .context(format!("Ill-formatted service name {service_name:?}"))?;
                    let start_time = {
                        let start_time = environ.get(&start_time_env).context(
                            format!("Service {service_name} with pid {pid} does not have {SUMO_AGENT_ENV_START_TIME} env")
                        )?;
                        let start_time = start_time.to_str().context(
                            format!("Service {service_name} with pid {pid} has invalid {SUMO_AGENT_ENV_START_TIME} env {start_time:?}")
                        )?;
                        chrono::DateTime::parse_from_rfc3339(start_time).context(
                            format!("Service {service_name} with pid {pid} has invalid {SUMO_AGENT_ENV_START_TIME} env {start_time}")
                        )?
                    };
                    let services: &mut Vec<_> =
                        found_services.entry(service_name.clone()).or_default();
                    services.push(RunningService {
                        service_name,
                        start_time,
                        pid,
                    });
                }
            }
        }
        Ok(found_services)
    }
}
