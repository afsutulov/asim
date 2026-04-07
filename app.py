from __future__ import annotations

import json
import logging
import os
import re
import secrets
import string
import uuid
from io import BytesIO
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import (
    Flask,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    session,
    url_for,
    g,
)
from werkzeug.exceptions import HTTPException

try:
    import crypt  # type: ignore
except ImportError:  # pragma: no cover
    crypt = None

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = Path(os.environ.get("ASIM_CONFIG", str(BASE_DIR / "data" / "config.json")))
DEFAULT_PROCESSING_PATH = Path(os.environ.get("ASIM_PROCESSING", str(BASE_DIR / "data" / "prosessing.json")))
DEFAULT_RESULTS_PATH = Path(os.environ.get("ASIM_RESULTS", str(BASE_DIR / "data" / "results.json")))

app = Flask(__name__)
app.secret_key = os.environ.get("ASIM_SECRET_KEY", "change-me-before-production")
app.config["JSON_AS_ASCII"] = False
app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024


class IgnoreHeadRootFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if '"HEAD / HTTP/' in message:
            return False
        return True


werkzeug_logger = logging.getLogger("werkzeug")
werkzeug_logger.addFilter(IgnoreHeadRootFilter())


class ConfigError(RuntimeError):
    pass


def relaxed_json_load(path: str | Path) -> Any:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Файл не найден: {p}")
    raw = p.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        cleaned = raw
        cleaned = re.sub(r"//.*?$", "", cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
        cleaned = re.sub(r'("\s*)(\n\s*")', r'\1,\2', cleaned)
        cleaned = re.sub(r'("[^"]+")\s+(?=[\[{\"])', r'\1: ', cleaned)
        cleaned = re.sub(r'((?:"[^"]*"|\d+|true|false|null|\}|\]))\s*(\n\s*")', r'\1,\2', cleaned, flags=re.I)
        cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)
        return json.loads(cleaned)


def save_json(path: str | Path, payload: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_path(path_str: str | None, fallback: Path | None = None) -> Path:
    if path_str:
        p = Path(path_str)
        if p.is_absolute() and p.exists():
            return p
        if not p.is_absolute():
            return (BASE_DIR / p).resolve()
        # absolute but absent: for packaged project prefer same layout under BASE_DIR
        if fallback is not None:
            return fallback
        return p
    if fallback is None:
        raise ConfigError("Не задан путь")
    return fallback


class RuntimeData:
    def __init__(self) -> None:
        self.config_path = DEFAULT_CONFIG_PATH


    def load_config(self) -> dict[str, Any]:
        data = relaxed_json_load(self.config_path)
        if not isinstance(data, dict):
            raise ConfigError("config.json должен содержать объект")
        return data

    def users_path(self, cfg: dict[str, Any]) -> Path:
        return resolve_path(cfg.get("users"), BASE_DIR / "data" / "users.json")

    def poligons_path(self, cfg: dict[str, Any]) -> Path:
        return resolve_path(cfg.get("poligons"), BASE_DIR / "data" / "poligons.json")

    def poli_root(self, cfg: dict[str, Any]) -> Path:
        return resolve_path(cfg.get("poli_path"), BASE_DIR / "poligons")

    def logs_root(self, cfg: dict[str, Any]) -> Path:
        return resolve_path(cfg.get("logs"), BASE_DIR / "logs")

    def load_users(self, cfg: dict[str, Any]) -> dict[str, Any]:
        path = self.users_path(cfg)
        if not path.exists():
            save_json(path, {})
        data = relaxed_json_load(path)
        if not isinstance(data, dict):
            raise ConfigError("users.json должен содержать объект")
        return data

    def save_users(self, cfg: dict[str, Any], payload: dict[str, Any]) -> None:
        save_json(self.users_path(cfg), payload)

    def load_poligons(self, cfg: dict[str, Any]) -> dict[str, Any]:
        path = self.poligons_path(cfg)
        if not path.exists():
            save_json(path, {})
        data = relaxed_json_load(path)
        if not isinstance(data, dict):
            raise ConfigError("poligons.json должен содержать объект")
        for item in data.values():
            if isinstance(item, dict) and "public" not in item and "publick" in item:
                item["public"] = item.pop("publick")
        return data

    def save_poligons(self, cfg: dict[str, Any], payload: dict[str, Any]) -> None:
        save_json(self.poligons_path(cfg), payload)

    def processing_path(self, cfg: dict[str, Any]) -> Path:
        return resolve_path(cfg.get("prosessing"), BASE_DIR / "data" / "prosessing.json")

    def results_json_path(self, cfg: dict[str, Any]) -> Path:
        return resolve_path(cfg.get("results"), BASE_DIR / "data" / "results.json")

    def results_zip_root(self, cfg: dict[str, Any]) -> Path:
        return resolve_path(cfg.get("results_path"), BASE_DIR / "results")

    def load_processing(self, cfg: dict[str, Any]) -> dict[str, Any]:
        path = self.processing_path(cfg)
        if not path.exists():
            save_json(path, {})
        data = relaxed_json_load(path)
        if not isinstance(data, dict):
            raise ConfigError("prosessing.json должен содержать объект")
        return data

    def save_processing(self, cfg: dict[str, Any], payload: dict[str, Any]) -> None:
        save_json(self.processing_path(cfg), payload)

    def load_results(self, cfg: dict[str, Any]) -> dict[str, Any]:
        path = self.results_json_path(cfg)
        if not path.exists():
            save_json(path, {})
        data = relaxed_json_load(path)
        if not isinstance(data, dict):
            raise ConfigError("results.json должен содержать объект")
        return data


runtime = RuntimeData()


def get_remote_addr() -> str:
    return request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")


def write_web_log(action: str, username: str = "-", success: bool = True, details: str = "") -> None:
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {'OK' if success else 'FAIL'} action={action} user={username or '-'} ip={get_remote_addr()}"
    if details:
        line += f" {details}"
    line += "\n"

    candidates: list[Path] = []
    try:
        cfg = runtime.load_config()
        candidates.append(runtime.logs_root(cfg) / "web.log")
    except Exception:
        pass
    candidates.append(BASE_DIR / "logs" / "web.log")

    for target in candidates:
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("a", encoding="utf-8") as fh:
                fh.write(line)
            return
        except Exception:
            continue


def wants_json_response() -> bool:
    return request.path.startswith("/api/") or request.headers.get("X-Requested-With") == "XMLHttpRequest"


def log_request_failure(status_code: int, details: str = "") -> None:
    try:
        username = session.get("username", "-")
    except Exception:
        username = "-"
    g.failure_logged = True
    write_web_log(f"http_{status_code}", username, False, f"method={request.method} path={request.path} {details}".strip())


def current_user() -> dict[str, Any] | None:
    username = session.get("username")
    if not username:
        return None
    cfg = runtime.load_config()
    users = runtime.load_users(cfg)
    user = users.get(username)
    if not user:
        session.clear()
        return None
    return {"username": username, **user}


@app.context_processor
def inject_layout_data() -> dict[str, Any]:
    return {
        "current_user": current_user(),
        "footer_address": 'Разработка и сопровождение: ГБУ "Центр информационного развития ПК" Пермского края',
    }


def verify_password(stored_password: str, provided_password: str) -> bool:
    if not stored_password:
        return False
    if stored_password == provided_password:
        return True
    if stored_password.startswith("$6$") and crypt is not None:
        return crypt.crypt(provided_password, stored_password) == stored_password
    return False


def hash_password(password: str) -> str:
    if crypt is None:
        return password
    salt = crypt.mksalt(crypt.METHOD_SHA512)
    return crypt.crypt(password, salt)


def generate_password(length: int = 14) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def require_login() -> dict[str, Any]:
    user = current_user()
    if not user:
        abort(401)
    return user


def require_any_group(*groups: str) -> dict[str, Any]:
    user = require_login()
    if user.get("group") not in groups:
        abort(403)
    return user


def is_admin(user: dict[str, Any]) -> bool:
    return user.get("group") == "admin"


def model_options(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    items = []
    for key, value in cfg.get("models", {}).items():
        items.append({"key": key, "description": value.get("description", key), "inputs": int(value.get("inputs", 1)), "season": value.get("season", "")})
    return sorted(items, key=lambda item: item["description"])


def processing_poligon_options(cfg: dict[str, Any]) -> list[dict[str, str]]:
    items = []
    for key, value in runtime.load_poligons(cfg).items():
        public = int(value.get("public", value.get("publick", 0)) or 0)
        if public not in (1, 2):
            continue
        items.append({"key": key, "name": value.get("name", key)})
    return items


def visible_poligons_for_user(cfg: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    poligons = runtime.load_poligons(cfg)
    if is_admin(user):
        return poligons
    result: dict[str, Any] = {}
    for key, value in poligons.items():
        if int(value.get("public", 0) or 0) == 2:
            result[key] = value
    return result


def format_date(date_str: str | None) -> str:
    if not date_str:
        return "—"
    for fmt_in, fmt_out in [
        ("%Y-%m-%d", "%d.%m.%Y"),
        ("%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M"),
        ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"),
    ]:
        try:
            return datetime.strptime(date_str, fmt_in).strftime(fmt_out)
        except ValueError:
            continue
    return date_str


def parse_dt(value: str | None) -> datetime:
    if not value:
        return datetime.min
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return datetime.min


def enrich_rows(source: dict[str, Any], cfg: dict[str, Any], include_download: bool = False) -> list[dict[str, Any]]:
    rows = []
    models = cfg.get("models", {})
    poligons = runtime.load_poligons(cfg)
    results_root = resolve_path(cfg.get("results"), BASE_DIR / "results")
    for item_uuid, item in source.items():
        if not isinstance(item, dict):
            continue
        row = {
            "uuid": item_uuid,
            "model": models.get(item.get("model"), {}).get("description", item.get("model", "—")),
            "poligon": poligons.get(item.get("poligon"), {}).get("name", item.get("poligon", "—")),
            "cloud": f"{item.get('cloud', '—')}%" if item.get("cloud") is not None else "—",
            "start": format_date(item.get("start")),
            "end": format_date(item.get("end")),
            "start2": format_date(item.get("start2")),
            "end2": format_date(item.get("end2")),
            "time": format_date(item.get("time")),
            "raw_time": item.get("time"),
        }
        if include_download:
            row["download_url"] = url_for("download_result", item_uuid=item_uuid)
            row["download_path"] = str(results_root / f"{item_uuid}.zip")
        rows.append(row)
    rows.sort(key=lambda x: parse_dt(x["raw_time"]), reverse=True)
    return rows


def tail_lines(path: Path, count: int = 500) -> str:
    if not path.exists():
        return f"Файл не найден: {path}"
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return ''.join(handle.readlines()[-count:])


def validate_identifier(identifier: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9]+", identifier))


def ensure_geojson_file(file_storage: Any) -> None:
    if not file_storage or not getattr(file_storage, "filename", ""):
        raise ConfigError("Нужно загрузить файл полигона")
    if not file_storage.filename.lower().endswith(".geojson"):
        raise ConfigError("Разрешены только файлы .geojson")




def load_geojson_payload() -> tuple[str, bytes]:
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        filename = (payload.get("file_name") or "polygon.geojson").strip()
        content = payload.get("file_content")
        if not filename.lower().endswith(".geojson"):
            raise ConfigError("Разрешены только файлы .geojson")
        if not content or not str(content).strip():
            raise ConfigError("Нужно загрузить файл полигона")
        try:
            json.loads(content)
        except Exception as exc:
            raise ConfigError(f"Файл полигона должен быть корректным GeoJSON: {exc}")
        return filename, str(content).encode("utf-8")

    file_storage = request.files.get("file")
    ensure_geojson_file(file_storage)
    payload = file_storage.read()
    if not payload:
        raise ConfigError("Файл полигона пустой")
    try:
        json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise ConfigError(f"Файл полигона должен быть корректным GeoJSON: {exc}")
    return file_storage.filename, payload


def save_geojson_bytes(target_file: Path, payload: bytes) -> None:
    target_file.parent.mkdir(parents=True, exist_ok=True)
    with target_file.open("wb") as fh:
        fh.write(payload)

def satellite_root(cfg: dict[str, Any]) -> Path:
    return resolve_path(cfg.get("satellite"), BASE_DIR / "satellite")


def build_file_entries(rel: str, root_dir: Path, browser_endpoint: str, download_endpoint: str) -> tuple[list[dict[str, Any]], str | None]:
    current_path = (root_dir / rel).resolve()
    try:
        current_path.relative_to(root_dir.resolve())
    except ValueError:
        abort(403)
    if not current_path.exists():
        abort(404)

    entries = []
    if current_path.is_dir():
        for entry in sorted(current_path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
            entries.append({
                "name": entry.name,
                "is_dir": entry.is_dir(),
                "size": entry.stat().st_size if entry.is_file() else None,
                "link": url_for(browser_endpoint, path=(Path(rel) / entry.name).as_posix()) if entry.is_dir() else url_for(download_endpoint, path=(Path(rel) / entry.name).as_posix()),
            })
    parent = None
    if rel:
        parent_rel = str(Path(rel).parent)
        parent = url_for(browser_endpoint, path="" if parent_rel == "." else parent_rel)
    return entries, parent


def safe_target(root_dir: Path, rel: str) -> Path:
    target = (root_dir / rel).resolve()
    try:
        target.relative_to(root_dir.resolve())
    except ValueError:
        abort(403)
    return target




@app.after_request
def after_request_logging(response):
    try:
        if response.status_code >= 400 and not getattr(g, "failure_logged", False) and request.endpoint != 'static':
            username = session.get("username", "-") if session else "-"
            extra = ""
            if response.content_type and response.content_type.startswith("application/json"):
                body = response.get_data(as_text=True)[:300].replace("\n", " ").strip()
                if body:
                    extra = f"response={body}"
            write_web_log(f"http_{response.status_code}", username, False, f"method={request.method} path={request.path} {extra}".strip())
    except Exception:
        pass
    return response

@app.errorhandler(401)
def unauthorized(_: Any):
    if wants_json_response():
        log_request_failure(401)
        return jsonify({"error": "Требуется авторизация"}), 401
    return redirect(url_for("login"))


@app.errorhandler(403)
def forbidden(error: Any):
    log_request_failure(403, f"reason={getattr(error, 'description', '')}")
    if wants_json_response():
        return jsonify({"error": "Доступ запрещён"}), 403
    return render_template("login.html", error="Доступ запрещён"), 403


@app.errorhandler(404)
def not_found(error: Any):
    log_request_failure(404, f"reason={getattr(error, 'description', '')}")
    if wants_json_response():
        return jsonify({"error": "Ресурс не найден"}), 404
    return render_template("login.html", error="Ресурс не найден"), 404


@app.errorhandler(413)
def too_large(error: Any):
    log_request_failure(413, "reason=payload_too_large")
    if wants_json_response():
        return jsonify({"error": "Размер загружаемого файла слишком большой"}), 413
    return render_template("login.html", error="Размер загружаемого файла слишком большой"), 413


@app.errorhandler(Exception)
def handle_exception(error: Exception):
    if isinstance(error, HTTPException):
        return error
    log_request_failure(500, f"reason={type(error).__name__}:{str(error)}")
    if wants_json_response():
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500
    return render_template("login.html", error="Внутренняя ошибка сервера"), 500


@app.route("/")
def root():
    user = current_user()
    if not user:
        return redirect(url_for("login"))
    if user.get("group") == "satellite":
        return redirect(url_for("satellite_browser"))
    return redirect(url_for("dashboard"))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        try:
            cfg = runtime.load_config()
            users = runtime.load_users(cfg)
        except Exception as exc:
            write_web_log("login", username, False, "reason=config_error")
            return render_template("login.html", error=str(exc)), 500
        user = users.get(username)
        if user and verify_password(user.get("password", ""), password):
            session.clear()
            session["username"] = username
            write_web_log("login", username, True)
            return redirect(url_for("root"))
        write_web_log("login", username, False, "reason=invalid_credentials")
        error = "Неверное имя пользователя или пароль"
    return render_template("login.html", error=error)


@app.route("/logout", methods=["POST"])
def logout():
    user = current_user()
    if user:
        write_web_log("logout", user["username"], True)
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
def dashboard():
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    return render_template("dashboard.html", user=user, model_options=model_options(cfg), poligon_options=processing_poligon_options(cfg))


@app.route("/api/session")
def api_session():
    user = require_any_group("admin", "user")
    return jsonify({"username": user["username"], "name": user.get("name", user["username"]), "group": user.get("group", "user")})


@app.route("/api/processing")
def api_processing():
    require_any_group("admin", "user")
    cfg = runtime.load_config()
    return jsonify(enrich_rows(runtime.load_processing(cfg), cfg))


@app.route("/api/results")
def api_results():
    require_any_group("admin", "user")
    cfg = runtime.load_config()
    return jsonify(enrich_rows(runtime.load_results(cfg), cfg, include_download=True))


@app.route("/api/logs")
def api_logs():
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    logs_root = runtime.logs_root(cfg)
    payload = {
        "asim": tail_lines(logs_root / "asim.log"),
        "task": tail_lines(logs_root / "task.log"),
    }
    if is_admin(user):
        payload["web"] = tail_lines(logs_root / "web.log")
        payload["cron"] = tail_lines(logs_root / "cron.log")
        payload["down"] = tail_lines(logs_root / "sentinel2-download.log")
    return jsonify(payload)



SEASON_DATES = {
    "spring": ("03-01", "05-31"),
    "summer": ("06-01", "08-31"),
    "autumn": ("09-01", "11-30"),
    "winter": ("12-01", "02-28"),
}

def season_to_dates(season: str, year: int) -> tuple[str, str]:
    start_md, end_md = SEASON_DATES[season]
    start = f"{year}-{start_md}"
    end = f"{year + 1}-{end_md}" if season == "winter" else f"{year}-{end_md}"
    return start, end

def available_years(cfg: dict[str, Any]) -> list[int]:
    sentinel_dir = resolve_path(cfg.get("sentinel") or cfg.get("satellite"), BASE_DIR / "sentinel")
    years = []
    if sentinel_dir.exists():
        for entry in sorted(sentinel_dir.iterdir()):
            if entry.is_dir() and entry.name.isdigit() and len(entry.name) == 4:
                if (entry / "cash.json").exists():
                    years.append(int(entry.name))
    return sorted(years)

@app.route("/api/available-years")
def api_available_years():
    require_any_group("admin", "user")
    return jsonify(available_years(runtime.load_config()))


@app.route("/api/processing/create", methods=["POST"])
def create_processing():
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    payload = request.get_json(silent=True) or {}
    model_key = payload.get("model")
    cloud = int(payload.get("cloud", 50))
    poligon = payload.get("poligon")

    if model_key not in cfg.get("models", {}):
        return jsonify({"error": "Неизвестная модель"}), 400
    if poligon not in {item['key'] for item in processing_poligon_options(cfg)}:
        return jsonify({"error": "Нужно выбрать локацию"}), 400
    if cloud < 0 or cloud > 100:
        return jsonify({"error": "Облачность должна быть от 0 до 100"}), 400

    model_spec = cfg["models"][model_key]
    is_two_input = int(model_spec.get("inputs", 1)) == 2
    season = model_spec.get("season", "")

    if season:
        # Сезонная модель: даты вычисляются из года.
        # Работает независимо от inputs — год выбирается всегда когда задан season.
        try:
            year = int(payload.get("year", 0))
        except (ValueError, TypeError):
            return jsonify({"error": "Укажите корректный год"}), 400
        if not year:
            return jsonify({"error": "Нужно выбрать год снимка"}), 400
        start, end = season_to_dates(season, year)
        start2, end2 = None, None
        if is_two_input:
            try:
                year2 = int(payload.get("year2", 0))
            except (ValueError, TypeError):
                return jsonify({"error": "Укажите корректный год базового снимка"}), 400
            if not year2:
                return jsonify({"error": "Нужно выбрать год базового снимка"}), 400
            if year == year2:
                return jsonify({"error": "Год нового и базового снимка должны отличаться"}), 400
            start2, end2 = season_to_dates(season, year2)
    else:
        start = payload.get("start")
        end = payload.get("end")
        start2 = payload.get("start2")
        end2 = payload.get("end2")
        if not start or not end:
            return jsonify({"error": "Нужно заполнить даты начала и завершения"}), 400
        if is_two_input and (not start2 or not end2):
            return jsonify({"error": "Для модели с двумя входами нужны start2 и end2"}), 400

    # Формат JSON остаётся прежним
    entry = {"model": model_key, "poligon": poligon, "cloud": cloud, "start": start, "end": end, "time": datetime.now().strftime("%Y-%m-%d %H:%M")}
    if is_two_input:
        entry["start2"] = start2
        entry["end2"] = end2

    data = runtime.load_processing(cfg)
    item_uuid = str(uuid.uuid4())
    data[item_uuid] = entry
    runtime.save_processing(cfg, data)
    write_web_log("processing_create", user["username"], True, f"uuid={item_uuid} model={model_key} poligon={poligon}")
    return jsonify({"ok": True, "uuid": item_uuid})


@app.route("/api/processing/delete/<item_uuid>", methods=["DELETE"])
def delete_processing(item_uuid: str):
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    data = runtime.load_processing(cfg)
    if item_uuid not in data:
        write_web_log("processing_delete", user["username"], False, f"uuid={item_uuid} reason=not_found")
        return jsonify({"error": "Запись не найдена"}), 404
    del data[item_uuid]
    runtime.save_processing(cfg, data)
    write_web_log("processing_delete", user["username"], True, f"uuid={item_uuid}")
    return jsonify({"ok": True})


@app.route("/api/results/delete/<item_uuid>", methods=["DELETE"])
def delete_result(item_uuid: str):
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    data = runtime.load_results(cfg)
    if item_uuid not in data:
        write_web_log("result_delete", user["username"], False, f"uuid={item_uuid} reason=not_found")
        return jsonify({"error": "Результат не найден"}), 404
    del data[item_uuid]
    save_json(runtime.results_json_path(cfg), data)
    result_file = runtime.results_zip_root(cfg) / f"{item_uuid}.zip"
    if result_file.exists():
        result_file.unlink()
    write_web_log("result_delete", user["username"], True, f"uuid={item_uuid}")
    return jsonify({"ok": True})


@app.route("/results/download/<item_uuid>")
def download_result(item_uuid: str):
    require_any_group("admin", "user")
    cfg = runtime.load_config()
    result_file = runtime.results_zip_root(cfg) / f"{item_uuid}.zip"
    if not result_file.exists():
        abort(404)
    return send_file(result_file, as_attachment=True, download_name=f"{item_uuid}.zip")


@app.route("/api/poligons")
def api_poligons():
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    data = visible_poligons_for_user(cfg, user)
    rows = []
    for key, value in data.items():
        rows.append({
            "id": key,
            "name": value.get("name", key),
            "public": int(value.get("public", 0) or 0),
            "file": f"{key}.geojson",
            "can_edit_id": is_admin(user),
            "can_edit_public": is_admin(user),
        })
    rows.sort(key=lambda x: x["name"].lower())
    return jsonify(rows)


@app.route("/api/poligons/create", methods=["POST"])
def api_poligons_create():
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    poligons = runtime.load_poligons(cfg)
    poli_root = runtime.poli_root(cfg)
    poli_root.mkdir(parents=True, exist_ok=True)
    try:
        source = request.get_json(silent=True) if request.is_json else request.form
        identifier = ((source.get("identifier") if source else None) or str(uuid.uuid4()).replace('-', '')).strip()
        name = ((source.get("name") if source else None) or "").strip()
        public = int((source.get("public") if source else None) or "2")
        if not is_admin(user):
            public = 2
        if not validate_identifier(identifier):
            raise ConfigError("Идентификатор должен содержать только латинские буквы и цифры")
        if public < 0 or public > 2:
            raise ConfigError("Публичность должна быть от 0 до 2")
        if not name:
            raise ConfigError("Нужно указать название полигона")
        if identifier in poligons:
            raise ConfigError("Полигон с таким идентификатором уже существует")
        if not poli_root.exists():
            poli_root.mkdir(parents=True, exist_ok=True)
        if not os.access(poli_root, os.W_OK):
            raise PermissionError(f"Нет прав на запись в каталог {poli_root}")
        _, geojson_bytes = load_geojson_payload()
        target_file = poli_root / f"{identifier}.geojson"
        save_geojson_bytes(target_file, geojson_bytes)
        poligons[identifier] = {"name": name, "public": public}
        runtime.save_poligons(cfg, poligons)
        write_web_log("poligon_create", user["username"], True, f"id={identifier} public={public} target={target_file}")
        return jsonify({"ok": True, "identifier": identifier})
    except PermissionError as exc:
        write_web_log("poligon_create", user["username"], False, f"reason=permission_error path={poli_root} detail={str(exc)}")
        return jsonify({"error": f"Нет прав на запись в каталог полигонов: {poli_root}"}), 500
    except Exception as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        write_web_log("poligon_create", user["username"], False, f"reason={type(exc).__name__}:{str(exc)}")
        return jsonify({"error": str(exc)}), status


@app.route("/api/poligons/<identifier>")
def api_poligon_detail(identifier: str):
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    poligons = visible_poligons_for_user(cfg, user)
    item = poligons.get(identifier)
    if not item:
        write_web_log("poligon_delete", user["username"], False, f"id={identifier} reason=not_found")
        return jsonify({"error": "Полигон не найден"}), 404
    file_exists = (runtime.poli_root(cfg) / f"{identifier}.geojson").exists()
    return jsonify({
        "id": identifier,
        "name": item.get("name", identifier),
        "public": int(item.get("public", 0) or 0),
        "file": f"{identifier}.geojson" if file_exists else "",
        "can_edit_id": is_admin(user),
        "can_edit_public": is_admin(user),
    })


@app.route("/api/poligons/<identifier>/update", methods=["POST"])
def api_poligon_update(identifier: str):
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    poligons = runtime.load_poligons(cfg)
    item = poligons.get(identifier)
    if not item:
        write_web_log("poligon_update", user["username"], False, f"id={identifier} reason=not_found")
        return jsonify({"error": "Полигон не найден"}), 404
    if not is_admin(user) and int(item.get("public", 0) or 0) != 2:
        write_web_log("poligon_update", user["username"], False, f"id={identifier} reason=forbidden")
        return jsonify({"error": "Недостаточно прав"}), 403

    poli_root = runtime.poli_root(cfg)
    poli_root.mkdir(parents=True, exist_ok=True)
    try:
        source = request.get_json(silent=True) if request.is_json else request.form
        new_identifier = ((source.get("identifier") if source else None) or identifier).strip()
        if not is_admin(user):
            new_identifier = identifier
        name = ((source.get("name") if source else None) or "").strip()
        public = int((source.get("public") if source else None) or str(item.get("public", 2)))
        if not is_admin(user):
            public = int(item.get("public", 2))
        replace_file = ((source.get("replace_file") if source else None) == "1") or bool((source.get("file_content") if source else None))
        remove_existing_file = ((source.get("remove_existing_file") if source else None) == "1")
        if not validate_identifier(new_identifier):
            raise ConfigError("Идентификатор должен содержать только латинские буквы и цифры")
        if public < 0 or public > 2:
            raise ConfigError("Публичность должна быть от 0 до 2")
        if not name:
            raise ConfigError("Нужно указать название полигона")
        if new_identifier != identifier and new_identifier in poligons:
            raise ConfigError("Полигон с таким идентификатором уже существует")

        old_file = poli_root / f"{identifier}.geojson"
        new_file = poli_root / f"{new_identifier}.geojson"
        if replace_file:
            _, geojson_bytes = load_geojson_payload()
            if old_file.exists() and old_file != new_file:
                old_file.unlink()
            save_geojson_bytes(new_file, geojson_bytes)
            if old_file.exists() and old_file == new_file:
                pass
        elif new_identifier != identifier and old_file.exists():
            old_file.rename(new_file)
        elif remove_existing_file:
            raise ConfigError("Нельзя сохранить полигон без файла")
        elif not new_file.exists() and not old_file.exists():
            raise ConfigError("Нужно загрузить файл полигона")

        if new_identifier != identifier:
            del poligons[identifier]
        poligons[new_identifier] = {"name": name, "public": public}
        runtime.save_poligons(cfg, poligons)
        write_web_log("poligon_update", user["username"], True, f"id={identifier} new_id={new_identifier} public={public}")
        return jsonify({"ok": True, "identifier": new_identifier})
    except Exception as exc:
        status = 403 if isinstance(exc, PermissionError) else 400
        write_web_log("poligon_update", user["username"], False, f"id={identifier} reason={type(exc).__name__}:{str(exc)}")
        return jsonify({"error": str(exc)}), status


@app.route("/api/poligons/<identifier>", methods=["DELETE"])
def api_poligon_delete(identifier: str):
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    poligons = runtime.load_poligons(cfg)
    item = poligons.get(identifier)
    if not item:
        return jsonify({"error": "Полигон не найден"}), 404
    if not is_admin(user) and int(item.get("public", 0) or 0) != 2:
        write_web_log("poligon_update", user["username"], False, f"id={identifier} reason=forbidden")
        return jsonify({"error": "Недостаточно прав"}), 403
    del poligons[identifier]
    runtime.save_poligons(cfg, poligons)
    geo = runtime.poli_root(cfg) / f"{identifier}.geojson"
    if geo.exists():
        geo.unlink()
    write_web_log("poligon_delete", user["username"], True, f"id={identifier}")
    return jsonify({"ok": True})


@app.route("/api/users")
def api_users():
    user = require_any_group("admin", "user")
    if not is_admin(user):
        return jsonify([])
    cfg = runtime.load_config()
    users = runtime.load_users(cfg)
    rows = []
    for username, data in users.items():
        rows.append({"username": username, "name": data.get("name", username), "group": data.get("group", "user")})
    rows.sort(key=lambda x: x["username"].lower())
    return jsonify(rows)


@app.route("/api/users/create", methods=["POST"])
def api_users_create():
    admin = require_any_group("admin")
    cfg = runtime.load_config()
    users = runtime.load_users(cfg)
    payload = request.get_json(silent=True) or {}
    username = (payload.get("username") or "").strip()
    name = (payload.get("name") or "").strip()
    group = (payload.get("group") or "user").strip()
    password = payload.get("password") or ""
    generate = bool(payload.get("generate_password"))
    if not validate_identifier(username):
        write_web_log("user_create", admin["username"], False, f"target={username} reason=invalid_username")
        return jsonify({"error": "Идентификатор пользователя должен содержать только латинские буквы и цифры"}), 400
    if group not in {"admin", "user", "satellite"}:
        write_web_log("user_create", admin["username"], False, f"target={username} reason=invalid_group")
        return jsonify({"error": "Недопустимая группа"}), 400
    if not name:
        write_web_log("user_create", admin["username"], False, f"target={username} reason=empty_name")
        return jsonify({"error": "Нужно указать имя пользователя"}), 400
    if username in users:
        write_web_log("user_create", admin["username"], False, f"target={username} reason=duplicate")
        return jsonify({"error": "Пользователь уже существует"}), 400
    if generate or not password:
        password = generate_password()
    users[username] = {"name": name, "group": group, "password": hash_password(password)}
    runtime.save_users(cfg, users)
    write_web_log("user_create", admin["username"], True, f"target={username} group={group}")
    return jsonify({"ok": True, "password": password})


@app.route("/api/users/<username>/update", methods=["POST"])
def api_users_update(username: str):
    admin = require_any_group("admin")
    cfg = runtime.load_config()
    users = runtime.load_users(cfg)
    if username not in users:
        write_web_log("user_update", admin["username"], False, f"target={username} reason=not_found")
        return jsonify({"error": "Пользователь не найден"}), 404
    payload = request.get_json(silent=True) or {}
    new_name = (payload.get("name") or "").strip()
    new_group = (payload.get("group") or users[username].get("group", "user")).strip()
    new_username = (payload.get("username") or username).strip()
    if not validate_identifier(new_username):
        write_web_log("user_update", admin["username"], False, f"target={username} reason=invalid_username")
        return jsonify({"error": "Идентификатор пользователя должен содержать только латинские буквы и цифры"}), 400
    if new_group not in {"admin", "user", "satellite"}:
        write_web_log("user_update", admin["username"], False, f"target={username} reason=invalid_group")
        return jsonify({"error": "Недопустимая группа"}), 400
    if not new_name:
        write_web_log("user_update", admin["username"], False, f"target={username} reason=empty_name")
        return jsonify({"error": "Нужно указать имя пользователя"}), 400
    if new_username != username and new_username in users:
        write_web_log("user_update", admin["username"], False, f"target={username} new_target={new_username} reason=duplicate")
        return jsonify({"error": "Пользователь с таким идентификатором уже существует"}), 400
    record = users.pop(username)
    record["name"] = new_name
    record["group"] = new_group
    users[new_username] = record
    runtime.save_users(cfg, users)
    write_web_log("user_update", admin["username"], True, f"target={username} new_target={new_username} group={new_group}")
    return jsonify({"ok": True})


@app.route("/api/users/<username>", methods=["DELETE"])
def api_users_delete(username: str):
    admin = require_any_group("admin")
    cfg = runtime.load_config()
    users = runtime.load_users(cfg)
    if username not in users:
        write_web_log("user_delete", admin["username"], False, f"target={username} reason=not_found")
        return jsonify({"error": "Пользователь не найден"}), 404
    if username == admin["username"]:
        write_web_log("user_delete", admin["username"], False, f"target={username} reason=self_delete")
        return jsonify({"error": "Нельзя удалить текущего пользователя"}), 400
    del users[username]
    runtime.save_users(cfg, users)
    write_web_log("user_delete", admin["username"], True, f"target={username}")
    return jsonify({"ok": True})


@app.route("/api/users/<username>/password", methods=["POST"])
def api_users_password(username: str):
    admin = require_any_group("admin")
    cfg = runtime.load_config()
    users = runtime.load_users(cfg)
    if username not in users:
        write_web_log("user_password_change", admin["username"], False, f"target={username} reason=not_found")
        return jsonify({"error": "Пользователь не найден"}), 404
    payload = request.get_json(silent=True) or {}
    password = payload.get("password") or ""
    if payload.get("generate_password") or not password:
        password = generate_password()
    users[username]["password"] = hash_password(password)
    runtime.save_users(cfg, users)
    write_web_log("user_password_change", admin["username"], True, f"target={username}")
    return jsonify({"ok": True, "password": password})


@app.route("/api/profile/password", methods=["POST"])
def api_profile_password():
    user = require_any_group("admin", "user")
    cfg = runtime.load_config()
    users = runtime.load_users(cfg)
    payload = request.get_json(silent=True) or {}
    old_password = payload.get("old_password") or ""
    new_password = payload.get("new_password") or ""
    confirm_password = payload.get("confirm_password") or ""
    record = users.get(user["username"], {})
    if not verify_password(record.get("password", ""), old_password):
        write_web_log("self_password_change", user["username"], False, "reason=old_password_invalid")
        return jsonify({"error": "Старый пароль указан неверно"}), 400
    if not new_password or new_password != confirm_password:
        write_web_log("self_password_change", user["username"], False, "reason=password_mismatch")
        return jsonify({"error": "Новые пароли не совпадают"}), 400
    users[user["username"]]["password"] = hash_password(new_password)
    runtime.save_users(cfg, users)
    write_web_log("self_password_change", user["username"], True)
    return jsonify({"ok": True})


@app.route("/satellite")
def satellite_browser():
    user = require_any_group("satellite")
    cfg = runtime.load_config()
    root_dir = satellite_root(cfg)
    rel = request.args.get("path", "").strip("/")
    entries, parent = build_file_entries(rel, root_dir, "satellite_browser", "satellite_download")
    return render_template("satellite.html", user=user, entries=entries, current_rel=rel or "/", parent=parent)


@app.route("/satellite/download")
def satellite_download():
    require_any_group("satellite")
    cfg = runtime.load_config()
    root_dir = satellite_root(cfg)
    rel = request.args.get("path", "").strip("/")
    target = safe_target(root_dir, rel)
    if not target.exists() or not target.is_file():
        abort(404)
    return send_from_directory(target.parent, target.name, as_attachment=True)


@app.route("/admin/snapshots")
def admin_snapshots():
    require_any_group("admin", "user")
    cfg = runtime.load_config()
    root_dir = satellite_root(cfg)
    rel = request.args.get("path", "").strip("/")
    entries, parent = build_file_entries(rel, root_dir, "admin_snapshots", "admin_snapshot_download")
    return render_template("snapshot_list.html", entries=entries, current_rel=rel or "/", parent=parent)


@app.route("/admin/snapshots/download")
def admin_snapshot_download():
    require_any_group("admin", "user")
    cfg = runtime.load_config()
    root_dir = satellite_root(cfg)
    rel = request.args.get("path", "").strip("/")
    target = safe_target(root_dir, rel)
    if not target.exists() or not target.is_file():
        abort(404)
    return send_from_directory(target.parent, target.name, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888, debug=False, use_reloader=False)