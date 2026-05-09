import Config

config :continuum,
  repo: nil,
  trusted_modules: [],
  untrusted_call_severity: :warn,
  determinism_violations: :error

import_config "#{config_env()}.exs"
