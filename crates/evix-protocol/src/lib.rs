use evix::{
  Config,
  Derivation,
  Diff,
  EvalError,
  Event,
  Filter,
  json as evix_json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Request {
  Eval {
    config: Config,
  },
  Watch {
    config: Config,
  },
  Query {
    config: Config,
    #[serde(default)]
    filter: Filter,
  },
  Diff {
    config: Config,
  },
}

impl Request {
  pub fn eval(config: &Config) -> Self {
    Self::Eval {
      config: config.clone(),
    }
  }

  pub fn watch(config: &Config) -> Self {
    Self::Watch {
      config: config.clone(),
    }
  }

  pub fn query(config: &Config, filter: &Filter) -> Self {
    Self::Query {
      config: config.clone(),
      filter: filter.clone(),
    }
  }

  pub fn diff(config: &Config) -> Self {
    Self::Diff {
      config: config.clone(),
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Response {
  Event {
    event: Json,
  },
  Diff {
    added:   Vec<Json>,
    removed: Vec<Json>,
    errors:  Vec<EvalError>,
  },
  Done,
  Error {
    message: String,
  },
}

impl Response {
  pub fn event(event: &Event) -> Self {
    Self::Event {
      event: evix_json::event_value(event),
    }
  }

  pub fn derivation_event(derivation: &Derivation) -> Self {
    Self::Event {
      event: evix_json::derivation_value(derivation),
    }
  }

  pub fn diff(diff: &Diff) -> Self {
    Self::Diff {
      added:   diff.added.iter().map(evix_json::derivation_value).collect(),
      removed: diff
        .removed
        .iter()
        .map(evix_json::derivation_value)
        .collect(),
      errors:  diff.errors.clone(),
    }
  }

  pub fn error(message: impl Into<String>) -> Self {
    Self::Error {
      message: message.into(),
    }
  }
}
