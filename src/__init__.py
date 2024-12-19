import base64
from datetime import datetime
import logging
import os
from pathlib import Path
from flask import Flask, jsonify, request
from fluidattacks_core.git import (
    download_repo_from_s3,
)
from typing import TypedDict

from git import Repo
from git import (
    NoSuchPathError,
)
from more_itertools import chunked
import fluidattacks_core.git as git_utils


class PayloadDownload(TypedDict):
    download_url: str
    group_name: str
    root_nickname: str


DESTINATION_BASE = os.environ.get("DESTINATION_BASE", "/home/ec2-user/groups")
app = Flask(__name__)

LOGGER = logging.getLogger(__name__)


async def download_repo(
    group_name: str,
    git_root_nickname: str,
    path_to_extract: str,
    *,
    download_url: str,
    gitignore: list[str] | None = None,
) -> Repo | None:
    repo_path = os.path.join(path_to_extract, git_root_nickname)
    await download_repo_from_s3(download_url, Path(repo_path), gitignore)
    try:
        return Repo(repo_path)
    except NoSuchPathError as error:
        LOGGER.error(
            error,
            extra={
                "group_name": group_name,
                "repo_nickname": git_root_nickname,
            },
        )
    return None


def get_recent_commit_after(
    repo: Repo, file_path: str, target_date: datetime, commit_a_hash: str
) -> tuple[str, str] | None:
    try:
        try:
            commit_a = repo.commit(commit_a_hash)
        except ValueError:
            return None

        # Ejecuta `git log --follow` para rastrear renombres y obtener commits
        logs = repo.git.log(
            "--reverse", "--follow", "--format=%H", "--name-status", "--", file_path
        )
        if not logs:
            return None

        commits_candidate = []
        last_known_path = file_path

        for commit_hash, _, file_changes in chunked(logs.split("\n"), 3):
            commit = repo.commit(commit_hash)
            change_parts = file_changes.split("\t")
            if change_parts[0].startswith("R"):  # Detecta renombrados
                old_path, new_path = change_parts[1:]
                if old_path == last_known_path:
                    last_known_path = new_path
            elif change_parts[0].startswith("C"):  # Detecta copias con cambios
                old_path, new_path = change_parts[1:]
                if old_path == last_known_path:
                    last_known_path = new_path
            elif change_parts[0] in {"A", "M"}:  # Agregado o modificado
                if change_parts[1] == last_known_path:
                    last_known_path = change_parts[1]

            # Filtrar commits posteriores al commit A
            if commit.committed_date > commit_a.committed_date:
                commit_date = commit.committed_datetime

                # Detén la búsqueda si el commit es igual o más reciente que la fecha objetivo
                if commit_date >= target_date:
                    break
                # Almacena los commits que cumplen la condición
                commits_candidate.append((commit, last_known_path))

        if not commits_candidate:
            return None

        # Devuelve el commit más reciente y la última ruta conocida
        most_recent_commit, final_path = commits_candidate[-1]
        return most_recent_commit.hexsha, final_path

    except Exception as e:
        print(f"Error: {e}")
        return None


def get_file_content(repo: Repo, commit_hash: str, file_path: str) -> bytes | None:
    try:
        commit = repo.commit(commit_hash)

        # Obtén el contenido del archivo en ese commit
        blob = commit.tree / file_path
        file_content = blob.data_stream.read()

        return file_content
    except Exception as exc:
        LOGGER.exception(exc)
        return None


async def download_repo_(
    group_name: str, git_root_nickname: str, download_url: str
) -> Repo | None:
    git_root_path = os.path.join(DESTINATION_BASE, group_name, git_root_nickname)
    if not os.path.exists(git_root_path):
        os.makedirs(
            os.path.join(DESTINATION_BASE, group_name),
            exist_ok=True,
        )
        repo = await download_repo(
            group_name=group_name,
            git_root_nickname=git_root_nickname,
            path_to_extract=os.path.join(DESTINATION_BASE, group_name),
            download_url=download_url,
        )
        return repo
    return Repo(git_root_path)


@app.route("/repos/mirror-exists/<group_name>/<git_root_nickname>", methods=["GET"])
async def mirror_exists(group_name: str, git_root_nickname: str):
    git_root_path = os.path.join(DESTINATION_BASE, group_name, git_root_nickname)
    print(git_root_path)
    return jsonify({"exists": os.path.exists(git_root_path)}), 200


@app.route(
    "/repos/file/rev/<group_name>/<git_root_nickname>/<file_path>/<rev>/<date_iso>",
    methods=["GET"],
)
async def get_recent_commit_after_(
    group_name: str, git_root_nickname: str, file_path: str, rev: str, date_iso: str
):
    git_root_path = os.path.join(DESTINATION_BASE, group_name, git_root_nickname)
    try:
        repo = Repo(git_root_path)
    except NoSuchPathError as error:
        LOGGER.error(
            error,
            extra={
                "group_name": group_name,
                "repo_nickname": git_root_nickname,
            },
        )
        return jsonify({"error": "No existe el repositorio."}), 404
    file_path = base64.b64decode(file_path).decode("utf-8")
    date = datetime.fromisoformat(date_iso)
    result = get_recent_commit_after(repo, file_path, date, rev)
    if not result:
        return jsonify({"error": "No se encontró un commit posterior."}), 404
    rev, file_path = result
    return jsonify({"rev": rev, "file_path": file_path}), 200


@app.route(
    "/repos/file/<group_name>/<git_root_nickname>/<file_path_b64>/<rev>",
    methods=["GET"],
)
async def get_file_content_by_rev(
    group_name: str, git_root_nickname: str, file_path_b64: str, rev: str
):
    file_path = base64.b64decode(file_path_b64).decode("utf-8")
    git_root_path = os.path.join(DESTINATION_BASE, group_name, git_root_nickname)
    try:
        repo = Repo(git_root_path)
    except NoSuchPathError as error:
        LOGGER.error(
            error,
            extra={
                "group_name": group_name,
                "repo_nickname": git_root_nickname,
            },
        )
        return jsonify({"error": "No existe el repositorio."}), 404
    content = get_file_content(repo, rev, file_path)
    if not content:
        return jsonify({"error": "No se encontró el archivo en el commit."}), 404

    return jsonify({"content": content.decode("utf-8")}), 200


@app.route("/repos/download", methods=["POST"])
async def handle_post():
    try:
        # Verificar si el payload es JSON
        if not request.is_json:
            return jsonify({"error": "El payload debe ser un JSON."}), 400

        # Obtener el JSON del cuerpo de la solicitud
        data: PayloadDownload = request.get_json()
        await download_repo_(
            group_name=data["group_name"],
            git_root_nickname=data["root_nickname"],
            download_url=data["download_url"],
        )

        # Realizar operaciones con los datos
        # Por ejemplo, devolver los datos procesados
        return jsonify({"message": "Datos recibidos correctamente.", "data": data}), 200

    except Exception as e:
        return jsonify({"error": f"Ocurrió un error: {str(e)}"}), 500


@app.route("/repos/rebase", methods=["POST"])
async def handle_rebase():
    try:
        # Verificar si el payload es JSON
        if not request.is_json:
            return jsonify({"error": "El payload debe ser un JSON."}), 400

        # Obtener el JSON del cuerpo de la solicitud
        data = request.get_json()
        group_name = data.get("group_name")
        git_root_nickname = data.get("root_nickname")
        path = data.get("path")
        line = data.get("line")
        rev_a = data.get("rev_a")
        rev_b = data.get("rev_b")
        ignore_errors = data.get("ignore_errors", True)

        if not all([group_name, git_root_nickname, path, line, rev_a, rev_b]):
            return jsonify({"error": "Faltan parámetros en el payload."}), 400

        git_root_path = os.path.join(DESTINATION_BASE, group_name, git_root_nickname)
        try:
            repo = Repo(git_root_path)
        except NoSuchPathError as error:
            LOGGER.error(
                error,
                extra={
                    "group_name": group_name,
                    "repo_nickname": git_root_nickname,
                },
            )
            return jsonify({"error": "No existe el repositorio."}), 404

        result = git_utils.rebase(
            repo,
            path=path,
            line=line,
            rev_a=rev_a,
            rev_b=rev_b,
            ignore_errors=ignore_errors,
        )

        if result is None:
            return jsonify({"error": "Rebase fallido."}), 500

        return jsonify({
            "path": result.path,
            "line": result.line,
            "rev": result.rev,
        }), 200

    except Exception as e:
        LOGGER.error(e)
        return jsonify({"error": "Error interno del servidor."}), 500


if __name__ == "__main__":
    app.run(debug=True)
