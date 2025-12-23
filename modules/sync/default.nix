self:
{
  config,
  pkgs,
  lib,
  ...
}:
{
  imports = [
    (import ./local.nix self)
    (import ./push.nix self)
    # (import ./pull.nix self) # not implemented
    # (import ./broker.nix self) # not implemented
  ];
}
