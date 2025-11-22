# MeshCoreBot
A bot for MeshCore that responds to commands, notably ping

This is still very much a work in progress. For now, the applicaiton only works with a USB companion. Bluetooth support coming soon.


Installation:
1. Clone the repository
2. Decide if you're going to use a virtual environment or not
3. Install dependencies: pip install -r requirements.txt
4. Edit conf.yaml
5. Connect device and start. python MeshCoreBot.py --port yourportgoeshere

Thanks to meshcore-dev for the python library: https://github.com/meshcore-dev/meshcore_py and chrisdavis2110 for the python implementation of MeshCore packet decoder: https://github.com/chrisdavis2110/meshcore-decoder-py

