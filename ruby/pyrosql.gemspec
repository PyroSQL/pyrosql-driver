Gem::Specification.new do |spec|
  spec.name          = "activerecord-pyrosql-adapter"
  spec.version       = "1.0.0"
  spec.authors       = ["PyroSQL Team"]
  spec.email         = ["team@pyrosql.com"]
  spec.summary       = "ActiveRecord adapter for PyroSQL"
  spec.description   = "ActiveRecord connection adapter for PyroSQL using the PWire binary protocol. Pure Ruby, no native extensions."
  spec.homepage      = "https://github.com/pyrosql/activerecord-pyrosql-adapter"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.1"

  spec.files         = Dir["lib/**/*.rb", "LICENSE", "README.md"]
  spec.require_paths = ["lib"]

  spec.add_dependency "activerecord", ">= 7.0", "< 8.1"
  spec.add_dependency "activesupport", ">= 7.0", "< 8.1"
  spec.add_dependency "extlz4", "~> 0.3"

  spec.add_development_dependency "rspec", "~> 3.12"
  spec.add_development_dependency "rake", "~> 13.0"
end
