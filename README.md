# homeassistant-4heat

Home Assistant Component for 4heat stove devices

<a href="https://www.buymeacoffee.com/zauberertz" target="_blank"><img height="41px" width="167px" src="https://cdn.buymeacoffee.com/buttons/default-blue.png" alt="Buy Me A Coffee"></a>

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg?style=for-the-badge)](https://github.com/custom-components/hacs)

### Installation

Copy this folder to `<config_dir>/custom_components/4heat/`.

### HACS
Search for 4Heat

### Configuration

The integration is configurated via UI

BETA * BETA * BETA - Not finished yet - BETA * BETA * BETA

### Todo:
- [x] Monitorig of all status values
- [ ] Discover more sensor meanings
- [ ] Find a way to get model and serial
- [x] Add Service to set value like target temp
- [x] Add Icons



### Local transport beta

The `improve-local-transport` branch adds bounded asynchronous communication,
optional direct state/error/water reads, background stove diagnostics and
observed command-result tracking while keeping existing entity identities.
See [DEPLOYMENT.md](DEPLOYMENT.md) for options, protocol limits, validation and
rollout/rollback instructions.
