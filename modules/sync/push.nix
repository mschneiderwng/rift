self:
{
  config,
  pkgs,
  lib,
  ...
}:
let
  cfg = config.services.rift.sync.push;
  rift = "${self.packages.${pkgs.stdenv.hostPlatform.system}.rift}";

  common = (import ../common.nix { inherit config pkgs lib; });
  inherit (common) allow unallow escapeUnitName;

  mkSync =
    cfg: remote: dataset:
    lib.escapeShellArgs (
      [
        "${rift}/bin/rift"
        "sync"
      ]
      ++ lib.optionals (cfg.filter != "") [
        "--filter"
        cfg.filter
      ]
      ++ lib.optional (cfg.verbosity != "") cfg.verbosity
      ++ cfg.extraArgs
      ++ builtins.concatMap (option: [
        "-S"
        option
      ]) cfg.zfsSendOptions
      ++ builtins.concatMap (option: [
        "-R"
        option
      ]) cfg.zfsRecvOptions
      ++ builtins.concatMap (option: [
        "-t"
        option
      ]) cfg.sshOptions
      ++ builtins.concatMap (pipe: [
        "-p"
        pipe
      ]) cfg.pipes
      ++ [
        "${dataset}"
        "${remote}/${dataset}"
      ]
    );

  mkSyncService =
    remote: cfg:
    let
      user = cfg.name;
    in
    {
      name = cfg.name;
      value = {
        description = "rift sync service";
        after = [ "zfs.target" ];
        path = [ pkgs.openssh ];
        startLimitBurst = 3;
        startLimitIntervalSec = 60 * 5;
        serviceConfig = {
          LoadCredential = [ "ssh_key:${cfg.sshPrivateKey}" ];
          User = user;
          Group = user;
          StateDirectory = [ "rift/${cfg.name}" ];
          StateDirectoryMode = "700";
          CacheDirectory = [ "rift/${cfg.name}" ];
          CacheDirectoryMode = "700";
          RuntimeDirectory = [ "rift/${cfg.name}" ];
          RuntimeDirectoryMode = "700";
          Type = "oneshot";
          Restart = "on-failure";
          RestartMode = "direct";
          RestartSec = "60";
          ExecStartPre = allow user [ "send" ] cfg.datasets;
          ExecStopPost = unallow user [ "send" ] cfg.datasets;
          ExecStart = map (mkSync cfg remote) cfg.datasets;
          CPUWeight = 20;
          CPUQuota = "75%";
          BindPaths = [ "/dev/zfs" ];
          DeviceAllow = [ "/dev/zfs" ];
          CapabilityBoundingSet = "";
          DevicePolicy = "closed";
          DynamicUser = true;
          LockPersonality = true;
          MemoryDenyWriteExecute = true;
          NoNewPrivileges = true;
          PrivateDevices = true;
          PrivateMounts = true;
          PrivateNetwork = false;
          PrivateTmp = true;
          PrivateUsers = false;
          ProtectClock = true;
          ProtectControlGroups = true;
          ProtectHome = true;
          ProtectHostname = true;
          ProtectKernelLogs = true;
          ProtectKernelModules = true;
          ProtectKernelTunables = true;
          ProtectProc = "invisible";
          ProtectSystem = "strict";
          RestrictAddressFamilies = [
            "AF_UNIX"
            "AF_INET"
            "AF_INET6"
          ];
          RestrictNamespaces = true;
          RestrictRealtime = true;
          RestrictSUIDSGID = true;
          SystemCallArchitectures = "native";
          SystemCallFilter = [
            " " # This is needed to clear the SystemCallFilter existing definitions
            "~@reboot"
            "~@swap"
            "~@obsolete"
            "~@mount"
            "~@module"
            "~@debug"
            "~@cpu-emulation"
            "~@clock"
            "~@raw-io"
            "~@privileged"
            "~@resources"
          ];
          UMask = 0077;
        };
      };
    };

  mkSyncTimer = remote: cfg: {
    name = cfg.name;
    value = {
      wantedBy = [ "timers.target" ];
      timerConfig = cfg.timerConfig;
    };
  };

in
{
  options.services.rift.sync.push = {

    enable = lib.mkEnableOption "Enable rift ZFS sync service";

    remotes = lib.mkOption {
      type = lib.types.attrsOf (
        lib.types.submodule (
          { remote, config, ... }:
          {
            options = {

              datasets = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                description = ''
                  List of local ZFS datasets that should be replicated to this remote.
                '';
                example = [
                  "rpool/.../dev"
                  "rpool/.../docs"
                ];
              };

              name = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                description = ''Systemd unit name.'';
                default = lib.mkDefault "rift-sync-${escapeUnitName remote}";
              };

              sshPrivateKey = lib.mkOption {
                type = lib.types.str;
                description = ''Passed to systemd LoadCredential.'';
              };

              sshOptions = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                description = ''Options passed to ssh.'';
                default = [
                  "ControlPath=/var/cache/rift/rift-sync-${config.name}/ssh-master"
                  "ControlMaster=auto"
                  "ControlPersist=60"
                  "IdentityFile=\${CREDENTIALS_DIRECTORY}/ssh_key"
                ];
              };

              filter = lib.mkOption {
                type = lib.types.str;
                description = ''A regex matching the snapshots to be sent.'';
                default = "rift_.*_.*(?<!frequently)$"; # all but frequently
              };

              pipes = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                example = [
                  "pv -p -e -t -r -a -b -s {size}"
                ];
                description = "Programs to pipe to between send and recv.";
              };

              zfsSendOptions = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                example = [ "-w" ];
                description = "Options passed to zfs send.";
              };

              zfsRecvOptions = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                example = [
                  "-s"
                  "-u"
                  "-F"
                ];
                description = "Options passed to zfs recv.";
              };

              verbosity = lib.mkOption {
                type = lib.types.str;
                description = ''Logging verbosity'';
                default = "-v";
              };

              extraArgs = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                description = "Extra rift arguments.";
              };

              timerConfig = lib.mkOption {
                type = lib.types.attrs;
                default = {
                  OnCalendar = "hourly";
                  RandomizedDelaySec = "10min";
                  Persistent = true;
                };
                description = "Systemd timer configuration.";
              };
            };
          }
        )
      );

      description = ''
        Mapping of remote rift receivers to their sync configuration.
      '';
      example = ''
        services.rift.sync.push.remotes."rift-recv@nas" = {
          datasets = [ "rpool/.../dev" "rpool/.../docs" ];
        };
      '';
    };

  };

  config = lib.mkIf cfg.enable {
    environment.systemPackages = with pkgs; [
      rift
      mbuffer
    ];

    users.groups."rift" = { };
    users.users."rift" = {
      group = "rift";
      isSystemUser = true;
    };

    systemd.timers = lib.mapAttrs' mkSyncTimer cfg.remotes;
    systemd.services = lib.mapAttrs' mkSyncService cfg.remotes;
  };
}
