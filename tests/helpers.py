import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

if importlib.util.find_spec("meshtastic") is None:
    meshtastic = types.ModuleType("meshtastic")
    tcp_interface = types.ModuleType("meshtastic.tcp_interface")
    mesh_interface = types.ModuleType("meshtastic.mesh_interface")

    class TCPInterface:
        pass

    class MeshInterface:
        class MeshInterfaceError(Exception):
            pass

    tcp_interface.TCPInterface = TCPInterface
    mesh_interface.MeshInterface = MeshInterface
    sys.modules.update({
        "meshtastic": meshtastic,
        "meshtastic.tcp_interface": tcp_interface,
        "meshtastic.mesh_interface": mesh_interface,
    })

if importlib.util.find_spec("pubsub") is None:
    pubsub = types.ModuleType("pubsub")
    pubsub.pub = types.SimpleNamespace(subscribe=lambda *args, **kwargs: None)
    sys.modules["pubsub"] = pubsub

if importlib.util.find_spec("openai") is None:
    openai = types.ModuleType("openai")
    openai.OpenAI = object
    sys.modules["openai"] = openai

from meshmind import bot as bot_module
from meshmind.bot import MeshmindBot
from meshmind.config import cfg


class BotTestCase(unittest.TestCase):
    """Create bots without touching runtime state, hardware, or the network."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.state_file = Path(self.temp_dir.name) / "alert_state.json"
        state_patch = patch.object(bot_module, "ALERT_STATE_FILE", self.state_file)
        state_patch.start()
        self.addCleanup(state_patch.stop)
        self.patch_cfg(BBS_ENABLED=False)

    def patch_cfg(self, **values):
        for name, value in values.items():
            config_patch = patch.object(cfg, name, value)
            config_patch.start()
            self.addCleanup(config_patch.stop)

    def make_bot(self, **config):
        if config:
            self.patch_cfg(**config)
        return MeshmindBot()


class FakeResponse:
    def __init__(self, payload=None, status_code=200, *, ok=None, text="payload"):
        self.payload = payload
        self.status_code = status_code
        self.ok = status_code == 200 if ok is None else ok
        self.text = text

    def json(self):
        if isinstance(self.payload, BaseException):
            raise self.payload
        return self.payload
