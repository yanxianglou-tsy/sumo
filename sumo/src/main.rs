mod agent;
mod service;
mod sumo_protocol;
mod supervisor;

use clap::Parser;

#[derive(Parser)]
enum Command {
    #[clap(display_order = 10, subcommand, about = "on-box agent")]
    Agent(agent::Command),
    #[clap(display_order = 20, subcommand, about = "supervisor")]
    Supervisor(supervisor::Command),
}

fn main() {
    match Command::parse() {
        Command::Agent(command) => {
            agent::run_command(command).unwrap();
        }
        Command::Supervisor(command) => {
            supervisor::run_command(command).unwrap();
        }
    }
}
