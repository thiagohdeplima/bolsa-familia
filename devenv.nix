{ pkgs, lib, config, inputs, ... }:

{
  packages = with pkgs; [ pandoc ];

  languages = {
    python = {
      enable = true;

      poetry = {
        enable = true;

        install.enable  = true;
        activate.enable = true;
      };
    };
  };

  processes = {
    jupyter = {
      exec = "poetry run jupyter lab --ServerApp.token=''";
    };
  };
}
