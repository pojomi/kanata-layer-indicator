// SPDX-License-Identifier: GPL-3.0
use cosmic::iced::Length;
use cosmic::iced::futures::SinkExt;
use cosmic::iced::futures::channel::mpsc::Sender;
use cosmic::iced::{Subscription, stream};
use cosmic::prelude::*;
use serde::Deserialize;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::sleep;

const KANATA_TCP_PORT: u16 = 5829;

#[derive(Debug, Deserialize)]
enum ServerMessage {
    LayerChange {
        new: String,
    },
    #[serde(other)]
    Other,
}

/// The application model stores app-specific state used to describe its interface and
/// drive its logic.
#[derive(Default)]
pub struct AppModel {
    /// Application state which is managed by the COSMIC runtime.
    core: cosmic::Core,
    current_text: String,
}

/// Messages emitted by the application and its widgets.
#[derive(Debug, Clone)]
pub enum Message {
    DataUpdate(String),
}

fn external_data_subscription() -> Subscription<Message> {
    Subscription::run(|| {
        stream::channel(100, |mut output: Sender<Message>| async move {
            loop {
                let stream = match TcpStream::connect(("127.0.0.1", KANATA_TCP_PORT)).await {
                    Ok(stream) => stream,
                    Err(_) => {
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
                let mut reader = BufReader::new(stream).lines();
                loop {
                    match reader.next_line().await {
                        Ok(Some(line)) => {
                            if let Ok(ServerMessage::LayerChange { new }) =
                                serde_json::from_str::<ServerMessage>(&line)
                            {
                                if output.send(Message::DataUpdate(new)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                sleep(Duration::from_secs(2)).await;
            }
        })
    })
}

/// Create a COSMIC application from the app model
impl cosmic::Application for AppModel {
    /// The async executor that will be used to run your application's commands.
    type Executor = cosmic::executor::Default;

    /// Data that your application receives to its init method.
    type Flags = ();

    /// Messages which the application and its widgets will emit.
    type Message = Message;

    /// Unique identifier in RDNN (reverse domain name notation) format.
    const APP_ID: &'static str = "com.github.pojomi.kanata-layer-indicator";

    fn core(&self) -> &cosmic::Core {
        &self.core
    }

    fn core_mut(&mut self) -> &mut cosmic::Core {
        &mut self.core
    }

    /// Initializes the application with any given flags and startup commands.
    fn init(
        core: cosmic::Core,
        _flags: Self::Flags,
    ) -> (Self, Task<cosmic::Action<Self::Message>>) {
        // Construct the app model with the runtime's core.
        let app = AppModel {
            core,
            current_text: "base".to_string(),
            ..Default::default()
        };

        (app, Task::none())
    }

    fn view(&self) -> Element<'_, Self::Message> {
        let text_widget = self.core.applet.text(&self.current_text);
        cosmic::widget::container(text_widget)
            .width(Length::Fill)
            .height(Length::Fill)
            .center_x(Length::Fill)
            .center_y(Length::Fill)
            .into()
    }

    fn subscription(&self) -> Subscription<Message> {
        external_data_subscription()
    }

    fn update(&mut self, message: Self::Message) -> Task<cosmic::Action<Self::Message>> {
        match message {
            Message::DataUpdate(new_text) => {
                self.current_text = new_text;
            }
        }
        Task::none()
    }

    fn style(&self) -> Option<cosmic::iced::theme::Style> {
        Some(cosmic::applet::style())
    }
}
