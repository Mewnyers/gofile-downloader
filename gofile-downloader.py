#! /usr/bin/env python3

import os
import sys
import requests
import threading
import hashlib
import shutil
import time
from pathlib import Path
from loguru import logger
from typing import Any, Dict, Optional, TypedDict
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import RequestException, ConnectTimeout

# ファイル情報の辞書構造を定義
class FileInfo(TypedDict):
    path: Path
    filename: str
    link: str
    id: str


def setup_logger():
    # remove existing handlers (to prevent redundant output)
    logger.remove()

    # コンソール出力用のフォーマット関数
    def console_format_function(record):
        return "<level>{message}</level>\n"

    # console output (colored)
    logger.add(
        sys.stdout,
        format=console_format_function,
        level="DEBUG",
        enqueue=False
    )

    # file output: with details (timestamp, level, caller, etc.
    logger.add(
        "app.log",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="10 MB",
        retention="7 days",
        encoding="utf-8",
        enqueue=True,
        backtrace=True,
        diagnose=True
    )


# increase max_workers for parallel downloads
# defaults to 3 download at time
class Main:
    def __init__(self, url: str, password: str | None = None, max_workers: int = 3) -> None:
        root_dir_str: str | None = os.getenv("GF_DOWNLOADDIR")
        
        if root_dir_str and Path(root_dir_str).is_dir():
            self._root_dir: Path = Path(root_dir_str)
        else:
            self._root_dir: Path = Path.cwd()

        # 基準パスの下に "Videos" フォルダを設定し、保存先ルートとする
        self._root_dir: Path = self._root_dir / "Videos"
        # Videosフォルダが存在しない場合は自動作成する
        self._root_dir.mkdir(exist_ok=True)

        self._url_or_file: str = url
        self._password: str | None = password
        self._lock: threading.Lock = threading.Lock()
        self._max_workers: int = max_workers

        # 停止フラグのインスタンス化
        self._stop_event: threading.Event = threading.Event()
        
        token: str | None = os.getenv("GF_TOKEN")
        self._token: str = token if token else self._get_token()

        self._content_dir: Optional[Path] = None

        # Keeps track of the number of recursion to get to the file
        self._recursive_files_index: int = 0

        # Dictionary to hold information about file and its directories structure
        # {"index": {"path": Path, "filename": "", "link": "", "id": ""}}
        self._files_info: Dict[str, FileInfo] = {}

    def run(self) -> None:
        """
        ダウンロード処理を実行します。
        """
        logger.info(f"Starting, please wait...")
        self._parse_url_or_file(self._url_or_file, self._password)


    def _threaded_downloads(self) -> None:
        """
        _threaded_downloads

        Parallelize the downloads.

        :return:
        """
        if not self._content_dir:
            logger.error(f"Content directory wasn't created, nothing done.")
            return

        executor = ThreadPoolExecutor(max_workers=self._max_workers)
        futures = []

        for item in self._files_info.values():
            if self._stop_event.is_set(): break
            futures.append(executor.submit(self._download_content, item))

        try:
            # 0.5秒おきにフラグと完了状態をチェックし、Ctrl+Cの割り込み余地を作る
            while not all(f.done() for f in futures):
                time.sleep(0.5)
                if self._stop_event.is_set():
                    break

        except KeyboardInterrupt:
            # 割り込み検知時のフラグセット
            logger.warning("\nUser interrupt detected in download loop.")
            self._stop_event.set()

        finally:
            # 安全なシャットダウン処理
            if self._stop_event.is_set():
                logger.warning("Stopping workers... (waiting for active downloads to pause)")
                executor.shutdown(wait=True, cancel_futures=True)
            else:
                executor.shutdown(wait=True)


    @staticmethod
    def _get_token() -> str:
        """
        _get_token

        Gets the access token of account created.

        :return: The access token of an account. Or exit if account creation fail.
        """

        user_agent: str | None = os.getenv("GF_USERAGENT")
        headers: dict[str, str] = {
            "User-Agent": user_agent if user_agent else "Mozilla/5.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*",
            "Connection": "keep-alive",
        }

        try:
            response = requests.post("https://api.gofile.io/accounts", headers=headers, timeout=50)
            response.raise_for_status()
            create_account_response: dict = response.json()

            if create_account_response["status"] != "ok":
                logger.error("Unexpected response status from GoFile API: %s", create_account_response)
                sys.exit(-1)

            token = create_account_response["data"]["token"]
            logger.debug("Successfully retrieved token from GoFile API.")
            return token

        except ConnectTimeout:
            logger.error("Connection to GoFile API timed out. Please check your network connection.")
        except RequestException as e:
            logger.error("Request to GoFile API failed: %s", e)
        except ValueError as e:
            logger.error("Response parsing error: %s", e)
        except Exception as e:
            logger.exception("Unexpected error occurred while retrieving GoFile token.")

        sys.exit(-1)
        
    def _get_fresh_download_link(self, file_id: str) -> Optional[str]:
        """
        _get_fresh_download_link

        ファイルIDを使用して、Gofile APIから新しいダウンロードリンクを要求します。
        CDNノードが故障している場合に、別のノードのリンクを取得するために使用します。

        :param file_id: GofileのファイルID
        :return: 新しいダウンロードリンク(str) または 失敗した場合は None
        """
        url: str = f"https://api.gofile.io/contents/{file_id}?wt=4fd6sg89d7s6&cache=true"
        user_agent: str | None = os.getenv("GF_USERAGENT")
        
        headers: dict[str, str] = {
            "User-Agent": user_agent if user_agent else "Mozilla/5.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*",
            "Connection": "keep-alive",
            "Authorization": f"Bearer {self._token}",
        }
        
        try:
            response_handler = requests.get(url, headers=headers, timeout=50)
            response_handler.raise_for_status()
            response: dict[Any, Any] = response_handler.json()

            if response["status"] == "ok" and response["data"]["type"] == "file":
                logger.debug(f"Successfully fetched new link for file ID {file_id}")
                return response["data"]["link"]
            else:
                logger.warning(f"API status not OK when fetching new link for {file_id}: {response.get('status')}")
                return None

        except RequestException as e:
            logger.error(f"Failed to fetch new link for file ID {file_id}: {e}")
            return None


    def _download_content(self, file_info: FileInfo, chunk_size: int = 16384) -> None:
        """
        _download_content

        Requests the contents of the file and writes it.
        (CDNノード迂回ロジックを実装)

        :param file_info: a dictionary with information about a file to be downloaded.
        :param chunk_size: the number of bytes it should read into memory.
        :return:
        """

        if self._stop_event.is_set():
            return

        file_path: Path = file_info["path"] / file_info["filename"]
        
        if file_path.exists() and file_path.stat().st_size > 0:
            with self._lock:
                sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                sys.stdout.flush()
                relative_path = file_path.relative_to(self._root_dir)
                logger.info(f"{relative_path} already exist, skipping.")
            return

        tmp_file: Path = file_path.with_suffix(f"{file_path.suffix}.part")
        
        file_id: str = file_info["id"]
        current_url: str = file_info["link"]
        
        user_agent: str | None = os.getenv("GF_USERAGENT")

        base_headers: dict[str, str] = {
            "Cookie": f"accountToken={self._token}",
            "Accept-Encoding": "gzip, deflate, br",
            "User-Agent": user_agent if user_agent else "Mozilla/5.0",
            "Accept": "*/*",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache"
        }
        
        max_link_refreshes: int = 2 # 元のリンク + 2回の新しいリンク
        
        for refresh_attempt in range(max_link_refreshes + 1):
            if refresh_attempt > 0:
                with self._lock:
                    sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                    sys.stdout.flush()
                    logger.warning(f"Attempting to get a new download link for {file_info['filename']} (Refresh {refresh_attempt}/{max_link_refreshes})")
                
                new_link = self._get_fresh_download_link(file_id)
                
                if not new_link or new_link == current_url:
                    with self._lock:
                        sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                        sys.stdout.flush()
                        logger.error(f"Failed to get a *new* link for {file_info['filename']}. Aborting download for this file.")
                    break # リンク再取得ループを抜ける
                
                current_url = new_link
                with self._lock:
                    logger.info(f"Got new link for {file_info['filename']}: {current_url.split('/')[2]}")

            # --- 現在のリンク (current_url) でのリトライ ---
            max_retries_per_link = 3
            
            for attempt in range(1, max_retries_per_link + 1):
                part_size: int = 0
                current_headers = base_headers.copy()
                # リンクごとにRefererとOriginを更新
                current_headers["Referer"] = f"{current_url}{('/' if not current_url.endswith('/') else '')}"
                current_headers["Origin"] = current_url
            
                if tmp_file.is_file():
                    try:
                        part_size = tmp_file.stat().st_size
                        if part_size > 0:
                            current_headers["Range"] = f"bytes={part_size}-"
                    except OSError as e:
                        with self._lock:
                            sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                            sys.stdout.flush()
                            logger.warning(f"Could not read size of '.part' file, attempting download from scratch: {e}")
                        part_size = 0

                try:
                    with requests.get(current_url, headers=current_headers, stream=True, timeout=(20, 60)) as response_handler:
                        status_code = response_handler.status_code
                        
                        if status_code == 416:
                            with self._lock:
                                sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                                sys.stdout.flush()
                                logger.warning(
                                    f"Attempt {attempt}/{max_retries_per_link}: Received status 416 (Range Not Satisfiable). "
                                    f"The '.part' file is likely corrupted. Deleting it and retrying."
                                )
                            if tmp_file.exists():
                                tmp_file.unlink()
                            time.sleep(1)
                            continue # 内側リトライ

                        if ((status_code in (403, 404, 405, 500)) or
                            (part_size == 0 and status_code != 200) or
                            (part_size > 0 and status_code != 206)):
                            with self._lock:
                                sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                                sys.stdout.flush()
                                logger.error(f"Attempt {attempt}/{max_retries_per_link}: Couldn't download the file from {current_url.split('/')[2]}. Status code: {status_code}")
                            continue # 内側リトライ

                        content_length: str | None = response_handler.headers.get("Content-Length")
                        content_range: str | None = response_handler.headers.get("Content-Range")
                        has_size_str: str | None = content_length if part_size == 0 else content_range.split("/")[-1] if content_range else None

                        # --- 改善点: CDNノード故障の検知 ---
                        if not has_size_str and status_code == 200:
                            with self._lock:
                                sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                                sys.stdout.flush()
                                logger.warning(
                                    f"Attempt {attempt}/{max_retries_per_link}: Faulty CDN Node detected for {file_info['filename']}. "
                                    f"Server OK (200) but no file size. Retrying on same link..."
                                )
                            if attempt < max_retries_per_link:
                                time.sleep(2 ** attempt)
                                continue # 内側リトライ
                            else:
                                logger.error(f"All {max_retries_per_link} attempts on this faulty link failed. Forcing link refresh.")
                                break # ★内側ループを抜け、外側ループ（リンク再取得）へ

                        if not has_size_str:
                            with self._lock:
                                sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                                sys.stdout.flush()
                                logger.error(f"Attempt {attempt}/{max_retries_per_link}: Couldn't find the file size from {current_url.split('/')[2]}. Status code: {status_code}")
                            continue # 内側リトライ
                        
                        # --- この時点で has_size_str は None ではない ---
                        total_size: float = float(has_size_str)

                        with open(tmp_file, "ab") as handler:
                            start_time: float = time.perf_counter()
                            for i, chunk in enumerate(response_handler.iter_content(chunk_size=chunk_size)):
                                if self._stop_event.is_set():
                                    # 停止フラグを検知したら、現在の進捗を保存したまま中断
                                    with self._lock:
                                        sys.stdout.write(f"\r[Aborted] {file_info['filename']} saved partially.\n")
                                    return 

                                if not chunk:
                                    continue
                                
                                handler.write(chunk)
                                current_downloaded_bytes = handler.tell()
                                total_downloaded_bytes = part_size + current_downloaded_bytes
                                
                                elapsed_time = time.perf_counter() - start_time
                                
                                if elapsed_time > 0:
                                    rate: float = current_downloaded_bytes / elapsed_time
                                    unit: str = "B/s"
                                    if rate >= 1024**3:
                                        rate /= 1024**3
                                        unit = "GB/s"
                                    elif rate >= 1024**2:
                                        rate /= 1024**2
                                        unit = "MB/s"
                                    elif rate >= 1024:
                                        rate /= 1024
                                        unit = "KB/s"
                                    
                                    with self._lock:
                                        terminal_width = shutil.get_terminal_size().columns
                                        progress: float = (total_downloaded_bytes / total_size) * 100
                                        message = (
                                            f"Downloading {file_info['filename']}: "
                                            f"{total_downloaded_bytes} of {int(total_size)} "
                                            f"{round(progress, 1)}% {round(rate, 1)}{unit} "
                                        )
                                        truncated_message = message[:terminal_width - 1]
                                        sys.stdout.write(f"\x1b[2K{truncated_message}\r")
                                        sys.stdout.flush()

                        final_size = tmp_file.stat().st_size
                        
                        if final_size == int(total_size):
                            with self._lock:
                                sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                                sys.stdout.flush()
                                logger.info(f"Downloading {file_info['filename']}: {final_size} of {int(total_size)} Done!")
                            shutil.move(tmp_file, file_path)
                            return # ★★★ ダウンロード成功、メソッドを終了 ★★★
                        else:
                            with self._lock:
                                sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                                sys.stdout.flush()
                                logger.warning(f"Downloaded size ({final_size}) does not match expected size ({int(total_size)}) for {file_info['filename']}. Retrying.")
                            continue # 内側リトライ

                except Exception as e:
                    with self._lock:
                        sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                        sys.stdout.flush()
                        logger.warning(f"Attempt {attempt}/{max_retries_per_link}: Error downloading {file_info['filename']}: {e}")
                
                if attempt < max_retries_per_link:
                    wait_time = 2 ** attempt
                    with self._lock:
                        sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
                        sys.stdout.flush()
                        logger.warning(f"Retrying ({attempt}/{max_retries_per_link})...")
                    time.sleep(wait_time)
            
            # --- 内側リトライループが終了 ---
            # (ここに到達した場合、このリンクでのダウンロードは失敗)
            
        # --- 外側リンク再取得ループが終了 ---
        with self._lock:
            sys.stdout.write(" " * shutil.get_terminal_size().columns + "\r")
            sys.stdout.flush()
            logger.error(f"Failed to download {file_info['filename']} after {max_link_refreshes + 1} different links.")


    def _parse_links_recursively(
        self,
        content_id: str,
        current_path: Path, # os.chdir の代わりにパスを引数で渡す
        password: str | None = None
    ) -> None:
        """
        _parse_links_recursively

        Parses for possible links recursively and populate a list with file's info
        while also creating directories and subdirectories.
        (★ フォルダ重複作成防止ロジックを追加)

        :param content_id: url to the content.
        :param current_path: 現在処理中のディレクトリパス
        :param password: content's password.
        :return:
        """

        url: str = f"https://api.gofile.io/contents/{content_id}?wt=4fd6sg89d7s6&cache=true"

        if password:
            url = f"{url}&password={password}"

        user_agent: str | None = os.getenv("GF_USERAGENT")

        headers: dict[str, str] = {
            "User-Agent": user_agent if user_agent else "Mozilla/5.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*",
            "Connection": "keep-alive",
            "Authorization": f"Bearer {self._token}",
        }

        try:
            response_handler = requests.get(url, headers=headers, timeout=50)
            response_handler.raise_for_status()
            response: dict[Any, Any] = response_handler.json()
        except RequestException as e:
            logger.error(f"Failed to fetch content info from {url}: {e}")
            return

        if response["status"] != "ok":
            logger.error(f"Failed to get a link as response from the {url}.")
            return

        data: dict[Any, Any] = response["data"]

        if "password" in data and "passwordStatus" in data and data["passwordStatus"] != "passwordOk":
            logger.warning(f"Password protected link. Please provide the password.")
            return

        if data["type"] == "folder":
            
            # --- 改善点: フォルダ重複作成防止 ---
            folder_path: Path
            if data["name"] == current_path.name:
                # APIが返したフォルダ名が、今いるフォルダ名と同じ (例: LNRSFY == LNRSFY)
                # これはルートフォルダ自身なので、サブディレクトリを作らない
                folder_path = current_path
            else:
                # これは本当のサブフォルダ
                folder_path = current_path / data["name"]
                folder_path.mkdir(exist_ok=True)
            # ------------------------------------

            for child_id in data["children"]:
                child: dict[Any, Any] = data["children"][child_id]

                if child["type"] == "folder":
                    # 再帰呼び出しで正しいパス(folder_path)を渡す
                    self._parse_links_recursively(child["id"], folder_path, password)
                else:
                    self._recursive_files_index += 1
                    self._files_info[str(self._recursive_files_index)] = {
                        "path": folder_path, # 正しいパス(folder_path)を保存
                        "filename": child["name"],
                        "link": child["link"],
                        "id": child["id"]
                    }
        else:
            # ルートにファイルが直接ある場合 (current_path に保存)
            self._recursive_files_index += 1
            self._files_info[str(self._recursive_files_index)] = {
                "path": current_path,
                "filename": data["name"],
                "link": data["link"],
                "id": data["id"]
            }

        # Count the frequency of each filename
        filename_count: dict[str, int] = {}
        for item in self._files_info.values():
            filename = item["filename"]
            filename_count[filename] = filename_count.get(filename, 0) + 1

        # Append the file ID to the filename only if the filename is duplicated
        for item in self._files_info.values():
            filename = item["filename"]
            if filename_count[filename] > 1:
                p_filename = Path(filename)
                new_stem = f"{p_filename.stem} ({item['id'][:8]})"
                item["filename"] = p_filename.with_stem(new_stem).name


    def _print_list_files(self) -> None:
        """
        _print_list_files

        Helper function to display a list of all files for selection.
        """
        if not self._files_info:
            logger.info("No files found to list.")
            return

        MAX_FILENAME_CHARACTERS: int = 100
        width: int = max(len(f"[{k}] -> ") for k in self._files_info.keys())

        for (k, v) in self._files_info.items():
            full_path: Path = v["path"] / v["filename"]
            relative_path_str: str = str(full_path.relative_to(self._root_dir))

            display_path: str = f"...{relative_path_str[-MAX_FILENAME_CHARACTERS:]}" \
                if len(relative_path_str) > MAX_FILENAME_CHARACTERS \
                else relative_path_str

            text: str =  f"{f'[{k}] -> '.ljust(width)}{display_path}"
            logger.info(f"{text}\n{'-' * len(text)}")


    def _download(self, url: str, password: str | None = None) -> None:
        """
        _download

        Requests to start downloading files.
        """

        try:
            if not url.split("/")[-2] == "d":
                logger.error(f"The url probably doesn't have an id in it: {url}.")
                return
            content_id: str = url.split("/")[-1]
        except IndexError:
            logger.error(f"{url} doesn't seem a valid url.")
            return

        _password: str | None = hashlib.sha256(password.encode()).hexdigest() if password else password

        logger.info(f"\nDownloading URL: {url}")

        # content_id に基づいてルートフォルダを(常に)作成
        self._content_dir = self._root_dir / content_id
        self._content_dir.mkdir(exist_ok=True)
        
        # _parse_links_recursively にベースパス(self._content_dir)を渡す
        self._parse_links_recursively(content_id, self._content_dir, _password)

        if not self._files_info:
            logger.error(f"No files found for url: {url}, nothing done.")
            try:
                if not any(self._content_dir.iterdir()):
                    self._content_dir.rmdir()
            except OSError as e:
                logger.warning(f"Could not remove empty directory {self._content_dir}: {e}")
            self._reset_class_properties()
            return

        interactive: bool = os.getenv("GF_INTERACTIVE") == "1"

        if interactive:
            self._print_list_files()

            input_str: str = input(
                f"Files to download (Ex: 1 3 7 | or leave empty to download them all)\n:: "
            )
            input_list: list[str] = input_str.split()
            valid_inputs: set[str] = set(input_list) & set(self._files_info.keys())

            if not input_str.strip() and self._files_info:
                logger.info("Downloading all files...")
            
            elif not valid_inputs:
                logger.info(f"No valid files selected. Nothing done.")
                # (空フォルダは削除せず残す)
                self._reset_class_properties()
                return
            
            elif valid_inputs:
                keys_to_delete: set[str] = set(self._files_info.keys()) - valid_inputs
                for key in keys_to_delete:
                    del self._files_info[key]

        self._threaded_downloads()
        if self._stop_event.is_set():
            logger.warning("Download Aborted by User.")
        else:
            logger.info(f"Download Completed!")
        self._reset_class_properties()


    def _parse_url_or_file(self, url_or_file: str, _password: str | None = None) -> None:
        """
        _parse_url_or_file

        Parses a file or a url for possible links.
        """
        
        url_file_path = Path(url_or_file)

        if not (url_file_path.exists() and url_file_path.is_file()):
            self._download(url_or_file, _password)
            return

        with open(url_file_path, "r") as f:
            lines: list[str] = f.readlines()

        for line in lines:
            line_splitted: list[str] = line.split(" ")
            url: str = line_splitted[0].strip()
            
            if not url:
                continue

            password: str | None = _password if _password else line_splitted[1].strip() \
                if len(line_splitted) > 1 else _password

            self._download(url, password)


    def _reset_class_properties(self) -> None:
        """
        _reset_class_properties

        Simply put the properties of the class to be used again for another link if necessary.
        """

        self._content_dir: Optional[Path] = None
        self._recursive_files_index: int = 0
        self._files_info.clear()


if __name__ == "__main__":
    try:
        setup_logger()
        
        url: str | None = None
        password: str | None = None
        argc: int = len(sys.argv)

        # 引数なしの処理
        if argc == 1:
            user_input = input("Please enter the URL or file path to download: ").strip()
            
            if user_input:
                parts = user_input.split(maxsplit=1)
                url = parts[0]
                if len(parts) > 1:
                    password = parts[1]
            else:
                sys.exit(0)

        # 引数ありの処理
        elif argc == 2:
            url = sys.argv[1]
        elif argc == 3:
            url = sys.argv[1]
            password = sys.argv[2]
        else:
            logger.info(f"Usage:\n"
                f"python gofile-downloader.py https://gofile.io/d/contentid\n"
                f"python gofile-downloader.py https://gofile.io/d/contentid password\n"
                f"python gofile-downloader.py /path/to/links.txt\n"
            )
            sys.exit(-1)

        if url:
            downloader = Main(url=url, password=password)
            downloader.run()

    except KeyboardInterrupt:
        pass
    
    finally:
        input("\nPress Enter to exit...")