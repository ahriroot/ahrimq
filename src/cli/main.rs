use amq::{
    message::{Message, ReqMsgAuthorizer, ReqMsgList},
    Config,
};
use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(unix)]
use tokio::net::UnixStream;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Parser, Debug)]
enum Commands {
    #[command(about = "Manage connections")]
    Connection {
        #[command(subcommand)]
        subcommand: ConnectionCommands,
    },
    #[command(about = "Manage topics")]
    Topic {
        #[command(subcommand)]
        subcommand: TopicCommands,
    },
    #[command(about = "Manage messages")]
    Message {
        #[command(subcommand)]
        subcommand: MessageCommands,
    },
    #[command(about = "Publish message")]
    Publish {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long)]
        message: String,
    },
    #[command(about = "Subscribe to topic")]
    Subscribe {
        #[arg(short, long)]
        topic: String,
    },
}

#[derive(Parser, Debug)]
enum ConnectionCommands {
    #[command(about = "List connections")]
    List,
}

#[derive(Parser, Debug)]
enum TopicCommands {
    #[command(about = "List topics")]
    List,
}

#[derive(Parser, Debug)]
enum MessageCommands {
    #[command(about = "List messages")]
    List {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long, default_value_t = 10)]
        page_size: u32,
        #[arg(short, long, default_value_t = 1)]
        page_num: u32,
    },
}



fn load_config(config_path: &str) -> Result<Config, Box<dyn Error>> {
    let content = std::fs::read_to_string(config_path)?;
    Ok(toml::from_str::<Config>(&content)?) 
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // Load configuration file
    let config = load_config(&args.config)?;
    println!("Using config file: {}", args.config);

    if let Some(command) = args.command {
        // Execute single command
        handle_command(&config, command).await?;
    } else {
        // Enter interactive mode
        println!("Entering interactive mode (type 'exit' to quit)");
        println!("Available commands: connection, topic, message, publish, subscribe");

        // Create rustyline editor
        let mut rl = DefaultEditor::new()?;
        
        // Load history
        let _ = rl.load_history("history.txt");

        loop {
            let readline = rl.readline("amqc> ");
            match readline {
                Ok(line) => {
                    let _ = rl.add_history_entry(line.as_str());
                    
                    let input = line.trim();
                    if input.is_empty() {
                        continue;
                    }

                    if input == "exit" || input == "quit" {
                        break;
                    }

                    // Parse command
                    let mut parts = input.split_whitespace();
                    let cmd_name = parts.next().unwrap_or("");

                    let cmd_args: Vec<&str> = parts.collect();

                    // Build command line arguments
                    let full_args: Vec<&str> = vec!["amqc", "-c", &args.config, cmd_name]
                        .into_iter()
                        .chain(cmd_args)
                        .collect();

                    match Args::try_parse_from(full_args) {
                        Ok(parsed_args) => {
                            if let Some(command) = parsed_args.command {
                                if let Err(e) = handle_command(&config, command).await {
                                    eprintln!("Failed to execute command: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to parse command: {}", e);
                            eprintln!("Please enter the correct command format");
                        }
                    }
                },
                Err(ReadlineError::Interrupted) => {
                    println!("\n^C");
                    break;
                },
                Err(ReadlineError::Eof) => {
                    println!("\nBye!");
                    break;
                },
                Err(err) => {
                    eprintln!("Failed to read command: {:?}", err);
                    break;
                }
            }
        }
        
        // Save history
        let _ = rl.save_history("history.txt");
    }

    Ok(())
}

async fn handle_command(config: &Config, command: Commands) -> Result<(), Box<dyn Error>> {
    // Use Unix socket if configured
    #[cfg(unix)]
    {
        let unix_path = config.get_unix_path();
        if !unix_path.is_empty() {
            println!("Connecting to Unix socket: {}", unix_path);
            let mut stream = UnixStream::connect(unix_path).await?;
            return process_with_stream(&mut stream, config, command).await;
        }
    }

    // Default to TCP
    let addr = config.get_address();
    println!("Connecting to TCP: {}", addr);
    let mut stream = TcpStream::connect(addr).await?;
    process_with_stream(&mut stream, config, command).await
}

async fn process_with_stream<S>(stream: &mut S, config: &Config, command: Commands) -> Result<(), Box<dyn Error>>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // Authorization
    let auth_msg = Message::ReqAuthorizer(ReqMsgAuthorizer {
        access_key: config.access_key.clone(),
        access_secret: config.access_secret.clone(),
    });
    let auth_bytes = auth_msg.serialize()?;

    stream.write_u32(auth_bytes.len() as u32).await?;
    stream.write_all(&auth_bytes).await?;

    // Read authorization response
    let len = stream.read_u32().await? as usize;
    let mut buf = vec![0; len];
    stream.read_exact(&mut buf).await?;

    // Process command
    match command {
        Commands::Connection { subcommand } => match subcommand {
            ConnectionCommands::List => {
                println!("List connections");

                // Send list connections request
                let msg = Message::ReqConnectionList(());
                let msg_bytes = msg.serialize()?;

                stream.write_u32(msg_bytes.len() as u32).await?;
                stream.write_all(&msg_bytes).await?;

                // Read response
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0; len];
                stream.read_exact(&mut buf).await?;

                let resp: Message = Message::deserialize(&buf)?;
                println!("{:?}", resp);
            }
        },
        Commands::Topic { subcommand } => match subcommand {
            TopicCommands::List => {
                println!("List topics");

                // Send list topics request
                let msg = Message::ReqTopicList(());
                let msg_bytes = msg.serialize()?;

                stream.write_u32(msg_bytes.len() as u32).await?;
                stream.write_all(&msg_bytes).await?;

                // Read response
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0; len];
                stream.read_exact(&mut buf).await?;

                let resp: Message = Message::deserialize(&buf)?;
                println!("{:?}", resp);
            }
        },
        Commands::Message { subcommand } => match subcommand {
            MessageCommands::List { topic, page_size, page_num } => {
                println!("List messages: topic={}, page_size={}, page_num={}", topic, page_size, page_num);

                // Send list messages request
                let msg = Message::ReqMessageList(ReqMsgList {
                    topic: topic.clone(),
                    page_size,
                    page_num,
                });
                let msg_bytes = msg.serialize()?;

                stream.write_u32(msg_bytes.len() as u32).await?;
                stream.write_all(&msg_bytes).await?;

                // Read response
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0; len];
                stream.read_exact(&mut buf).await?;

                let resp: Message = Message::deserialize(&buf)?;
                println!("{:?}", resp);
            }
        },
        Commands::Publish { topic, message } => {
            println!("Publish message: topic={}, message={}", topic, message);

            // Send publish message request
            let msg = Message::ReqPublish(amq::message::ReqMsgPublish {
                topic: topic,
                message: message.into_bytes(),
            });
            let msg_bytes = msg.serialize()?;

            stream.write_u32(msg_bytes.len() as u32).await?;
            stream.write_all(&msg_bytes).await?;

            // Read response
            let len = stream.read_u32().await? as usize;
            let mut buf = vec![0; len];
            stream.read_exact(&mut buf).await?;

            let resp: Message = Message::deserialize(&buf)?;
            println!("{:?}", resp);
        },
        Commands::Subscribe { topic } => {
            println!("Subscribe to topic: topic={}", topic);
            println!("Note: Subscribe requires continuous listening, please run this command in a new window");

            // Send subscribe request
            let msg = Message::ReqSubscribeTopic(amq::message::ReqMsgSubscriber {
                topic: topic.clone(),
            });
            let msg_bytes = msg.serialize()?;

            stream.write_u32(msg_bytes.len() as u32).await?;
            stream.write_all(&msg_bytes).await?;

            // Read subscribe response
            let len = stream.read_u32().await? as usize;
            let mut buf = vec![0; len];
            stream.read_exact(&mut buf).await?;

            let resp: Message = Message::deserialize(&buf)?;
            println!("{:?}", resp);

            // Start listening for messages
            println!("Listening for messages on topic {}... (Press Ctrl+C to exit)", topic);
            loop {
                let len = stream.read_u32().await? as usize;
                let mut buf = vec![0; len];
                stream.read_exact(&mut buf).await?;

                let msg: Message = Message::deserialize(&buf)?;
                println!("Received message: {:?}", msg);
            }
        },
    }

    Ok(())
}
