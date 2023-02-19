//! Key design points - heavily influenced by http://supervisord.org/
//!
//! - each server has an agent
//! - each server agent has an RPC server
//!
//! master takes a config file, dispatch to each agent
//!
//! key scenarios:
//! 1. master heartbeats agents and alerts if it's not running
//! 2. agents are crontab started each week
//! 3. master loads config, request the agent to start/stop the service
//! 4. master streams updates from agents which includes service statuses / resource usage, etc.
//!
//! Service definition should include the following
//! 1. service name
//! 2. executable and arguments
//! 3. environmental variables
//! 4. working directory
//! 5. stdout / stderr redirection
//! 6. cpu affinity
//! 7. known TCP host ports - for address conflict detection
//!
//! Useful tools:
//! 1. given a list of hostnames (e.g. uxshkelctpm00{1,2}), generate a host definition including
//! - interfaces / IP addresses
//! - CPUs / isolcpus
//!
//! Failure stories:
//! 1. What if someone tries to start a service that is already started?
//! - this will be detected by the pid file
//! 2. What if one service crashes/restarts?
//! - the agent detects this and proactively pushes a state update to the master
//! 3. What if the agent crashes?
//! - the master should be able to find out this becasue of connection lost / timeout, and raise alert, someone needs to start the agent again
//! 4. What happens when an agent restarts? It needs to find out the existing running services
//! - by maintaining pid files for existing services
//! 5. What happens if the master dies and restarts?
//! - connect to all agents and find out the running services
//! - cross check with its master record and request start/stop.
//!
//! Agent startup:
//! 1. check pid directory and see if any services are already running, add to monitor set
//! 2. start RPC server, wait for requests to start/stop services
//! 3. start task to periodically gather service status changes
//! 4. start task to periodically gather service resource usage
//!
//! Random ideas:
//! 1. configurable shutdown grace period, (sigterm then sigkill)
//! 2. whitelist of environment variables to be inherited by the child process
//!

use std::fmt;
use std::net::SocketAddrV4;
use std::{collections::BTreeMap, path::PathBuf, str::FromStr};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ServiceName(String);

impl FromStr for ServiceName {
    type Err = anyhow::Error;

    fn from_str(name: &str) -> anyhow::Result<Self> {
        let re = regex::Regex::new(r"^[a-z][a-z0-9\-]*[a-z0-9]$").unwrap();
        if !re.is_match(name) {
            anyhow::bail!("Invalid service name {}", name);
        }
        Ok(Self(name.to_owned()))
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// TODO: support sys_on_range

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceDefinition {
    pub name: ServiceName,
    pub runs_on: SocketAddrV4,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub envs: BTreeMap<String, String>,
    pub working_directory: PathBuf,
    pub redirect_stdout_to: PathBuf,
    pub redirect_stderr_to: PathBuf,
    pub cpu_affinity: Vec<u32>,
}

// #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
// pub struct ServiceIncarnationKey {
//     service_name: ServiceName,
//     start_time: chrono::DateTime<chrono::FixedOffset>,
// }

// impl ServiceIncarnationKey {
//     pub fn new(
//         service_name: ServiceName,
//         start_time: chrono::DateTime<chrono::FixedOffset>,
//     ) -> Self {
//         Self {
//             service_name,
//             start_time,
//         }
//     }
// }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunningService {
    pub service_name: ServiceName,
    pub pid: i32,
    pub start_time: chrono::DateTime<chrono::FixedOffset>,
}
