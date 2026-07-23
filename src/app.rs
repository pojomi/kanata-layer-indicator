// SPDX-License-Identifier: GPL-3.0

use crate::config::Config;
use cosmic::cosmic_config::{self, CosmicConfigEntry};
use cosmic::iced::Alignment::Center;
use cosmic::iced::futures::SinkExt;
use cosmic::iced::futures::channel::mpsc::Sender;
use cosmic::iced::{Subscription, stream, window::Id};
use cosmic::prelude::*;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

/// The application model stores app-specific state used to describe its interface and
/// drive its logic.
#[derive(Default)]
pub struct AppModel {
    /// Application state which is managed by the COSMIC runtime.
    core: cosmic::Core,
    /// The popup id.
    popup: Option<Id>,
    /// Configuration data that persists between application runs.
    config: Config,
    /// Example row toggler.
    current_text: String,
}

/// Messages emitted by the application and its widgets.
#[derive(Debug, Clone)]
pub enum Message {
    PopupClosed(Id),
    UpdateConfig(Config),
    DataUpdate(String),
}

fn external_data_subscription() -> Subscription<Message> {
    Subscription::run(|| {
        stream::channel(100, |mut output: Sender<Message>| async move {
            let child = Command::new("journalctl")
                .args(&["-f", "-u", "kanata", "-n", "5"]) // Note: "-n" and "5" often need to be separate args
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .expect("didnt work");

            let stdout = child.stdout.expect("no stdout");
            let mut reader = BufReader::new(stdout).lines();

            loop {
                match reader.next_line().await {
                    Ok(Some(buf)) => {
                        // Check if the line contains the layer definition
                        if buf.contains("(deflayer ") {
                            let layer: Vec<&str> = buf.split(' ').collect();
                            if let Some(layer_name) = layer.last() {
                                let _ = output
                                    .send(Message::DataUpdate(layer_name.trim().to_string()))
                                    .await;
                                // DO NOT RETURN HERE. Continue the loop to catch future changes.
                            }
                        }
                    }
                    Ok(None) | Err(_) => {
                        // Process ended or error occurred
                        let _ = output.close().await;
                        return ();
                    }
                }
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
    const APP_ID: &'static str = "com.github.pojomi.keyd-layer-indicator";

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
            config: cosmic_config::Config::new(Self::APP_ID, Config::VERSION)
                .map(|context| match Config::get_entry(&context) {
                    Ok(config) => config,
                    Err((_errors, config)) => {
                        // for why in errors {
                        //     tracing::error!(%why, "error loading app config");
                        // }

                        config
                    }
                })
                .unwrap_or_default(),
            current_text: format!("base"),
            ..Default::default()
        };

        (app, Task::none())
    }

    fn on_close_requested(&self, id: Id) -> Option<Message> {
        Some(Message::PopupClosed(id))
    }

    /// Describes the interface based on the current state of the application model.
    ///
    /// The applet's button in the panel will be drawn using the main view method.
    /// This view should emit messages to toggle the applet's popup window, which will
    /// be drawn using the `view_window` method.
    fn view(&self) -> Element<'_, Self::Message> {
        let text_widget = cosmic::widget::text::body(&self.current_text)
            .align_x(Center)
            .align_y(Center);

        cosmic::widget::column::with_children(vec![text_widget.into()]).into()
    }

    /// Register subscriptions for this application.
    ///
    /// Subscriptions are long-lived async tasks running in the background which
    /// emit messages to the application through a channel. They may be conditionally
    /// activated by selectively appending to the subscription batch, and will
    /// continue to execute for the duration that they remain in the batch.
    fn subscription(&self) -> Subscription<Message> {
        external_data_subscription()
    }

    /// Handles messages emitted by the application and its widgets.
    ///
    /// Tasks may be returned for asynchronous execution of code in the background
    /// on the application's async runtime. The application will not exit until all
    /// tasks are finished.
    fn update(&mut self, message: Self::Message) -> Task<cosmic::Action<Self::Message>> {
        match message {
            Message::DataUpdate(new_text) => {
                self.current_text = new_text;
            }
            Message::UpdateConfig(config) => {
                self.config = config;
            }
            Message::PopupClosed(id) => {
                if self.popup.as_ref() == Some(&id) {
                    self.popup = None;
                }
            }
        }
        Task::none()
    }

    fn style(&self) -> Option<cosmic::iced::theme::Style> {
        Some(cosmic::applet::style())
    }
}
