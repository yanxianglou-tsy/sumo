use crate::service::ServiceName;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AgentError {
    Disconnected,
    PollingServicesError { err: String },
    MissingService { service_name: ServiceName },
    ExtraService { service_name: ServiceName },
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    #[clap(display_order = 10, about = "Run sumo supervisor")]
    Server(server::Args),
    #[clap(display_order = 20, subcommand, about = "client tools")]
    Client(client::Command),
}

pub fn run_command(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Server(args) => server::run(args),
        Command::Client(command) => tokio::runtime::Runtime::new()
            .expect("Failed to start tokio runtime")
            .block_on(async move { client::run_command(command).await }),
    }
}

pub mod server {
    use std::{
        collections::{BTreeMap, BTreeSet},
        net::SocketAddrV4,
        path::PathBuf,
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use anyhow::Context;
    use rustc_hash::FxHashMap;
    use tokio_stream::StreamExt;
    use tonic::transport::Uri;
    use tracing::{error, info, warn};

    use crate::{
        agent::ServiceUpdate,
        service::RunningService,
        sumo_protocol::{
            agent_rpc_client::AgentRpcClient,
            supervisor_rpc_server::{SupervisorRpc, SupervisorRpcServer},
            AgentStartServiceRequest, LoginRequest, StartServiceResponse, StopServiceRequest,
            StopServiceResponse,
        },
    };
    use crate::{
        service::{ServiceDefinition, ServiceName},
        sumo_protocol::SupervisorStartServiceRequest,
    };

    use super::AgentError;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Config {
        base_directory: Option<PathBuf>,
        service_definitions: BTreeMap<ServiceName, ServiceDefinition>,
        managed_hosts: BTreeSet<SocketAddrV4>,
        log_level: String,
    }
    // TODO: validate config to make sure
    // - service name in key & def are consistent
    // - all hosts in service def are in managed_hosts

    #[derive(Debug, clap::Args)]
    pub struct Args {
        #[clap(long)]
        config_file: PathBuf,
    }

    pub struct Supervisor {
        service_definitions: BTreeMap<ServiceName, ServiceDefinition>,
        agent_errors: FxHashMap<SocketAddrV4, AgentError>,
        running_services: FxHashMap<SocketAddrV4, BTreeMap<ServiceName, Vec<RunningService>>>,
    }

    pub fn run(args: Args) -> anyhow::Result<()> {
        let config: Config = {
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
                PathBuf::from_str(&format!("/home/{user}/app/sumo"))?
            }
        };
        std::fs::create_dir_all(&base_directory).context("Creating base directory")?;

        daemonize::Daemonize::new()
            .working_directory(&base_directory)
            .pid_file(base_directory.join("supervisor.pid"))
            .chown_pid_file(true)
            .start()
            .context("Failed to daemonize supervisor process")?;

        run_impl(base_directory, config)
    }

    #[tokio::main]
    async fn run_impl(base_directory: PathBuf, config: Config) -> anyhow::Result<()> {
        let logfile = tracing_appender::rolling::daily(&base_directory, "supervisor-log");
        let log_level = tracing::Level::from_str(&config.log_level).context("Invalid log level")?;
        tracing_subscriber::fmt()
            .with_writer(logfile)
            .with_max_level(log_level)
            .with_ansi(false)
            .init();

        info!("Supervisor starting up with config {config:?}");

        let supervisor = Arc::new(Mutex::new(Supervisor {
            service_definitions: config.service_definitions.clone(),
            running_services: FxHashMap::default(),
            agent_errors: FxHashMap::default(),
        }));

        subscribe_to_all_agent_updates(&config, supervisor.clone());
        start_rpc_server(supervisor.clone());

        wait_for_sigterm().await;
        Ok(())
    }

    fn start_rpc_server(supervisor: Arc<Mutex<Supervisor>>) {
        let addr: SocketAddrV4 = "0.0.0.0:9527".parse().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(SupervisorRpcServer::new(supervisor))
                .serve(addr.into())
                .await
                .expect("Error running supervisor RPC server")
        });
        info!("Started RPC server listening on {addr}");
    }

    fn subscribe_to_all_agent_updates(config: &Config, supervisor: Arc<Mutex<Supervisor>>) {
        for &host in &config.managed_hosts {
            let uri = Uri::builder()
                .scheme("http")
                .authority(host.to_string())
                .path_and_query("/")
                .build()
                .unwrap();
            tokio::task::spawn({
                let supervisor = supervisor.clone();
                async move {
                    loop {
                        match subscribe_to_one_agent_updates(host, uri.clone(), supervisor.clone())
                            .await
                        {
                            Ok(()) => {
                                warn!(
                                    "Lost agent updates connection to {uri}, will retry connecting"
                                );
                            }
                            Err(err) => {
                                error!("Agent {uri} updates subscription failure with error {err}, will retry connecting");
                            }
                        }
                        {
                            let mut supervisor =
                                supervisor.lock().expect("Failed to lock supervisor mutex");
                            supervisor
                                .agent_errors
                                .insert(host, AgentError::Disconnected);
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    }
                }
            });
        }
    }

    async fn subscribe_to_one_agent_updates(
        host: SocketAddrV4,
        uri: Uri,
        supervisor: Arc<Mutex<Supervisor>>,
    ) -> anyhow::Result<()> {
        let mut client = AgentRpcClient::connect(uri.clone())
            .await
            .context(format!("Connecting to {uri}"))?;
        let username = nix::unistd::User::from_uid(nix::unistd::getuid())
            .context("Failed to get user name")?
            .unwrap()
            .name;
        let request = tonic::Request::new(LoginRequest { username });
        let mut response = client
            .login(request)
            .await
            .context(format!("Log into agent {host}"))?
            .into_inner();
        info!("Subscribed to agent {uri} for updates");
        while let Some(update) = response.next().await {
            let update: ServiceUpdate = serde_json::from_str(&update?.service_update_json)?;
            info!("Got agent {uri} update: {update:?}");
            let mut supervisor = supervisor.lock().expect("Failed to lock supervisor mutex");
            match update {
                ServiceUpdate::AllServicesReport { services } => {
                    supervisor.running_services.insert(host, services);
                    supervisor.agent_errors.remove(&host);
                }
                ServiceUpdate::PollingServicesError { err } => {
                    supervisor.running_services.remove(&host);
                    supervisor
                        .agent_errors
                        .insert(host, AgentError::PollingServicesError { err });
                }
            }
        }
        Ok(())
    }

    #[tonic::async_trait]
    impl SupervisorRpc for Arc<Mutex<Supervisor>> {
        async fn start_service(
            &self,
            request: tonic::Request<SupervisorStartServiceRequest>,
        ) -> Result<tonic::Response<StartServiceResponse>, tonic::Status> {
            info!("Got request to start service {:?}", request);
            let service_name = ServiceName::from_str(&request.into_inner().service_name).unwrap();

            let def = {
                let supervisor = self.lock().unwrap();

                if supervisor
                    .running_services
                    .iter()
                    .any(|(_host, services_on_host)| services_on_host.contains_key(&service_name))
                {
                    return Result::Err(tonic::Status::failed_precondition(format!(
                        "Service {service_name} is already running.",
                    )));
                };

                match supervisor.service_definitions.get(&service_name) {
                    None => {
                        return Result::Err(tonic::Status::invalid_argument(format!(
                            "Unknown service {service_name}"
                        )));
                    }
                    Some(def) => def.clone(),
                }
            };

            let mut client = {
                let uri = Uri::from_str(&format!("http://{}", def.runs_on)).unwrap();
                AgentRpcClient::connect(uri)
                    .await
                    .map_err(|err| tonic::Status::from_error(Box::new(err)))?
            };
            let request = tonic::Request::new(AgentStartServiceRequest {
                service_definition_json: serde_json::to_string(&def).unwrap(),
            });
            client.start_service(request).await
        }

        async fn stop_service(
            &self,
            request: tonic::Request<StopServiceRequest>,
        ) -> Result<tonic::Response<StopServiceResponse>, tonic::Status> {
            info!("Got request to stop service {:?}", request);
            let service_name = ServiceName::from_str(&request.into_inner().service_name).unwrap();
            let host = {
                let supervisor = self.lock().unwrap();

                supervisor
                    .running_services
                    .iter()
                    .find_map(|(&host, services_on_host)| {
                        if services_on_host.contains_key(&service_name) {
                            Some(host)
                        } else {
                            None
                        }
                    })
            };
            if host.is_none() {
                return Result::Err(tonic::Status::invalid_argument(format!(
                    "Service {service_name} is not running.",
                )));
            };
            let mut client = {
                let uri = Uri::from_str(&format!("http://{}", host.unwrap())).unwrap();
                match AgentRpcClient::connect(uri).await {
                    Ok(client) => client,
                    Err(err) => {
                        return Result::Err(tonic::Status::from_error(Box::new(err)));
                    }
                }
            };
            let request = tonic::Request::new(StopServiceRequest {
                service_name: service_name.to_string(),
            });
            let response = client.stop_service(request).await?;
            Ok(response)
        }
    }

    async fn wait_for_sigterm() {
        use tokio::signal::unix::*;
        let mut signals = signal(SignalKind::terminate()).unwrap();
        signals.recv().await;
    }
}

pub mod client {

    #[derive(Debug, clap::Subcommand)]
    pub enum Command {
        #[clap(display_order = 10, about = "Start a service")]
        StartService(start::Args),
        #[clap(display_order = 20, about = "Stop a service")]
        StopService(stop::Args),
        #[clap(display_order = 30, about = "Subscribe to service updates")]
        SubscribeUpdates(updates::Args),
    }

    pub async fn run_command(command: Command) -> anyhow::Result<()> {
        match command {
            Command::StartService(args) => start::run(args).await,
            Command::StopService(args) => stop::run(args).await,
            Command::SubscribeUpdates(args) => updates::run(args).await,
        }
    }

    mod updates {
        use tokio_stream::StreamExt;
        use tonic::transport::Uri;

        use crate::sumo_protocol::{agent_rpc_client::AgentRpcClient, LoginRequest};

        #[derive(Debug, clap::Args)]
        pub struct Args {
            #[clap(display_order = 10)]
            agent_rpc_address: Uri,
        }

        pub async fn run(args: Args) -> anyhow::Result<()> {
            let mut client = AgentRpcClient::connect(args.agent_rpc_address).await?;
            let username = nix::unistd::User::from_uid(nix::unistd::getuid())?
                .expect("Failed to get user name")
                .name;
            let request = tonic::Request::new(LoginRequest { username });
            let mut response = client.login(request).await?.into_inner();
            while let Some(update) = response.next().await {
                let update = update.unwrap();
                println!("Got update: {update:?}");
            }
            Ok(())
        }
    }

    mod start {
        use crate::{
            service::ServiceName,
            sumo_protocol::{
                supervisor_rpc_client::SupervisorRpcClient, SupervisorStartServiceRequest,
            },
        };

        #[derive(Debug, clap::Args)]
        pub struct Args {
            #[clap(long)]
            pub service_name: ServiceName,
        }

        pub async fn run(args: Args) -> anyhow::Result<()> {
            let mut client = SupervisorRpcClient::connect("http://127.0.0.1:9527").await?;
            let request = tonic::Request::new(SupervisorStartServiceRequest {
                service_name: args.service_name.to_string(),
            });
            let response = client.start_service(request).await?;
            println!("Response: {response:?}");

            Ok(())
        }
    }

    mod stop {
        use crate::{
            service::ServiceName,
            sumo_protocol::{supervisor_rpc_client::SupervisorRpcClient, StopServiceRequest},
        };

        #[derive(Debug, clap::Args)]
        pub struct Args {
            #[clap(long)]
            pub service_name: ServiceName,
        }

        pub async fn run(args: Args) -> anyhow::Result<()> {
            let mut client = SupervisorRpcClient::connect("http://127.0.0.1:9527").await?;
            let request = tonic::Request::new(StopServiceRequest {
                service_name: args.service_name.to_string(),
            });
            let response = client.stop_service(request).await?;
            println!("Response: {response:?}");

            Ok(())
        }
    }
}
