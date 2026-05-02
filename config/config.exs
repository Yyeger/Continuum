import Config

config :continuum,
  repo: nil,
  trusted_modules: [],
  determinism_violations: :error

import_config "#{config_env()}.exs"
