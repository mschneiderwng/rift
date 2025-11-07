{
  description = "deep learning development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    flake-parts.url = "github:hercules-ci/flake-parts";

    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      flake-parts,
      uv2nix,
      pyproject-nix,
      pyproject-build-systems,
      ...
    }:

    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = nixpkgs.lib.systems.flakeExposed;
      imports = [
        inputs.flake-parts.flakeModules.easyOverlay
        inputs.treefmt-nix.flakeModule
      ];
      perSystem =
        {
          self',
          system,
          lib,
          config,
          pkgs,
          ...
        }:

        let
          inherit (nixpkgs) lib;
          # Load a uv workspace from a workspace root.
          # Uv2nix treats all uv projects as workspace projects.
          workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

          # Create package overlay from workspace.
          overlay = workspace.mkPyprojectOverlay {
            # Prefer prebuilt binary wheels as a package source.
            # Sdists are less likely to "just work" because of the metadata missing from uv.lock.
            # Binary wheels are more likely to, but may still require overrides for library dependencies.
            sourcePreference = "wheel"; # or sourcePreference = "sdist";
          };

          pkgs = import nixpkgs {
            system = "x86_64-linux";
            config = {
              allowUnfree = true;
            };
          };

          python = pkgs.python312;

          pyprojectOverrides = final: prev: {
            textual = prev.textual.overrideAttrs (old: {
              buildInputs = (old.buildInputs or [ ]) ++ [
                final.setuptools
              ];
            });
          };

          # Construct package set
          baseSet = pkgs.callPackage pyproject-nix.build.packages {
            python = python;
          };

          pythonSet =
            # Use base package set from pyproject.nix builders
            baseSet.overrideScope (
              lib.composeManyExtensions [
                pyproject-build-systems.overlays.default
                overlay
                pyprojectOverrides
              ]
            );

          # Create an overlay enabling editable mode for all local dependencies.
          editableOverlay = workspace.mkEditablePyprojectOverlay {
            root = "$REPO_ROOT";
          };

          # Override previous set with our overrideable overlay.
          editablePythonSet = pythonSet.overrideScope (
            lib.composeManyExtensions [
              editableOverlay

              # Apply fixups for building an editable package of your workspace packages
              (final: prev: {
                rift = prev.rift.overrideAttrs (old: {
                  # It's a good idea to filter the sources going into an editable build
                  # so the editable package doesn't have to be rebuilt on every change.
                  src = lib.fileset.toSource {
                    root = old.src;
                    fileset = lib.fileset.unions [
                      (old.src + "/pyproject.toml")
                      (old.src + "/README.md")
                      (old.src + "/src/rift/__init__.py")
                    ];
                  };

                  # Hatchling (our build system) has a dependency on the `editables` package when building editables.
                  #
                  # In normal Python flows this dependency is dynamically handled, and doesn't need to be explicitly declared.
                  # This behaviour is documented in PEP-660.
                  #
                  # With Nix the dependency needs to be explicitly declared.
                  nativeBuildInputs = old.nativeBuildInputs ++ final.resolveBuildSystem { editables = [ ]; };
                });
              })
            ]
          );
        in
        {
          apps.default = {
            type = "app";
            program = "${self.packages.${system}.default}/bin/rift";
          };

          packages = {
            default = python.pkgs.buildPythonApplication {
              pname = "rift";
              version = "0.01";
              pyproject = true;
              src = ./.;
              propagatedBuildInputs = with python.pkgs; [
                attrs
                click
                multimethod
                precisely
                setuptools
                structlog
              ];
            };
            package = pythonSet.mkVirtualEnv "rift-env" workspace.deps.default; # this code + deps
            venv = editablePythonSet.mkVirtualEnv "rift-venv" workspace.deps.all; # deps only
          };

          # # Pytest flake using uv2nix
          # checks =
          #   let
          #     test-deps = editablePythonSet.mkVirtualEnv "rift-test-env" { rift = [ "test" ]; };
          #     typing-deps = editablePythonSet.mkVirtualEnv "rift-typing-env" { rift = [ "typing" ]; };
          #   in
          #   {
          #     pytest = pkgs.runCommand "pytest-check" { buildInputs = [ test-deps ]; } ''
          #       pytest ${self}
          #       mkdir $out
          #     '';
          #     ty = pkgs.runCommand "ty-check" { buildInputs = [ typing-deps ]; } ''
          #       ty check ${self}
          #       mkdir $out
          #     '';
          #   };

          # run with nix fmt
          treefmt.config = {
            projectRootFile = "flake.nix";
            programs.nixfmt.enable = false;
            programs.ruff.check = true;
            programs.ruff.format = true;
            programs.isort.enable = true;
          };

          # This example provides two different modes of development:
          # - Impurely using uv to manage virtual environments
          # - Pure development using uv2nix to manage virtual environments
          devShells = {
            # It is of course perfectly OK to keep using an impure virtualenv workflow and only use uv2nix to build packages.
            # This devShell simply adds Python and undoes the dependency leakage done by Nixpkgs Python infrastructure.
            impure = pkgs.mkShell {
              name = "impure shell";
              packages = [
                python
                pkgs.uv
              ];
              env = {
                UV_PYTHON_DOWNLOADS = "never"; # Prevent uv from managing Python downloads
                UV_PYTHON = python.interpreter; # Force uv to use nixpkgs Python interpreter
              }
              // lib.optionalAttrs pkgs.stdenv.isLinux {
                # Python libraries often load native shared objects using dlopen(3).
                # Setting LD_LIBRARY_PATH makes the dynamic library loader aware of libraries without using RPATH for lookup.
                LD_LIBRARY_PATH = lib.makeLibraryPath pkgs.pythonManylinuxPackages.manylinux1;
              };
              shellHook = ''
                unset PYTHONPATH
              '';
            };

            # This devShell uses uv2nix to construct a virtual environment purely from Nix, using the same dependency specification as the application.
            # The notable difference is that we also apply another overlay here enabling editable mode ( https://setuptools.pypa.io/en/latest/userguide/development_mode.html ).
            #
            # This means that any changes done to your local files do not require a rebuild.
            uv2nix =
              let
                venv = editablePythonSet.mkVirtualEnv "rift-dev-env" workspace.deps.all;

              in
              pkgs.mkShell {
                name = "uv2nix shell";
                packages = [
                  venv
                  pkgs.uv
                  pkgs.ty
                  pkgs.jetbrains.pycharm-professional
                ];

                env = {
                  UV_NO_SYNC = "1"; # Don't create venv using uv
                  UV_PYTHON = "${venv}/bin/python"; # Force uv to use Python interpreter from venv
                  UV_PYTHON_DOWNLOADS = "never"; # Prevent uv from downloading managed Python's
                  LD_LIBRARY_PATH = lib.makeLibraryPath ([ venv ] ++ pkgs.pythonManylinuxPackages.manylinux1); # the latter one is hardly useful unless we use uv directly and patchelf has not run yet
                };

                shellHook = ''
                  # Undo dependency propagation by nixpkgs.
                  unset PYTHONPATH
                  # Get repository root using git. This is expanded at runtime by the editable `.pth` machinery.
                  export REPO_ROOT=$(git rev-parse --show-toplevel)
                  # Make all exported dev bins accessible
                  source ${venv}/bin/activate
                '';
              };
          };
        };
    };
}
