import yaml, os

def load_config(env: str) -> dict:
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "conf", "config.yaml")
    cfg_path = os.path.abspath(cfg_path)
    with open(cfg_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if env not in data["envs"]:
        raise KeyError(f"Unknown env: {env}")
    env_cfg = data["envs"][env]
    settings = data.get("settings", {})
    return {"env": env, **env_cfg, **settings}
