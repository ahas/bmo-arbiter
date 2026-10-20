//! The one shape every failure and refusal is answered in, and its codes, each answering the status it is
//! sent with (docs/api.md, 「The wire format」).

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Why a request was not answered with its route's own answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorCode {
  BadRequest,
  Unauthorized,
  NoRoute,
  NotFound,
  Conflict,
  Gone,
  NameRule,
  BadField,
  NoPlace,
  Failed,
  NotReady,
}

impl ErrorCode {
  /// Every code, in the order the document lists them.
  pub const ALL: [Self; 11] = [
    Self::BadRequest,
    Self::Unauthorized,
    Self::NoRoute,
    Self::NotFound,
    Self::Conflict,
    Self::Gone,
    Self::NameRule,
    Self::BadField,
    Self::NoPlace,
    Self::Failed,
    Self::NotReady,
  ];

  /// The code as it crosses the wire.
  pub fn as_str(self) -> &'static str {
    match self {
      Self::BadRequest => "bad-request",
      Self::Unauthorized => "unauthorized",
      Self::NoRoute => "no-route",
      Self::NotFound => "not-found",
      Self::Conflict => "conflict",
      Self::Gone => "gone",
      Self::NameRule => "name-rule",
      Self::BadField => "bad-field",
      Self::NoPlace => "no-place",
      Self::Failed => "failed",
      Self::NotReady => "not-ready",
    }
  }

  /// The HTTP status the code is answered with.
  pub fn status(self) -> u16 {
    match self {
      Self::BadRequest => 400,
      Self::Unauthorized => 401,
      Self::NoRoute | Self::NotFound => 404,
      Self::Conflict => 409,
      Self::Gone => 410,
      Self::NameRule | Self::BadField | Self::NoPlace => 422,
      Self::Failed => 500,
      Self::NotReady => 503,
    }
  }

  /// When the code is answered, as the OpenAPI document describes the answer.
  pub fn meaning(self) -> &'static str {
    match self {
      Self::BadRequest => {
        "the body, the query or a key in the path is not the shape the route takes; the error names what is \
         wrong"
      }
      Self::Unauthorized => "the master's token is missing or wrong",
      Self::NoRoute => "no route answers this method and path",
      Self::NotFound => {
        "nothing stands at the key the path names: no placement, ticket or card unit of that id or name"
      }
      Self::Conflict => {
        "the member's state does not allow the change: an unload that left the unit on the card, a restore \
         that found no room, a rest ended by a change"
      }
      Self::Gone => "an event stream resumed from before the window the master keeps",
      Self::NameRule => "a service's name is not lower-case letters, digits and hyphens",
      Self::BadField => {
        "a field breaks a rule of its own: a description's unit not of its service, or a detail that does not \
         read; the error names it"
      }
      Self::NoPlace => "a ticket for a service with no place among the node's occupants",
      Self::Failed => "the program failed; the error is the failure as a sentence",
      Self::NotReady => "the node arbiter is starting and moves no unit yet",
    }
  }
}

impl fmt::Display for ErrorCode {
  fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
    out.write_str(self.as_str())
  }
}

/// The body of every answer that is not the route's own: its code and an English sentence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "Error")]
pub struct WireError {
  pub code: ErrorCode,
  pub error: String,
}

impl WireError {
  pub fn new(code: ErrorCode, error: impl Into<String>) -> Self {
    Self {
      code,
      error: error.into(),
    }
  }
}

impl fmt::Display for WireError {
  fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(out, "{} ({})", self.error, self.code)
  }
}
