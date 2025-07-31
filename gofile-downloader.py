#! /usr/bin/env python3


import os
import sys
import requests
import threading
import platform
import hashlib
import shutil
import time
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import RequestException, ConnectTimeout


def setup_logger():
    # remove existing handlers (to prevent redundant output)
    logger.remove()

    # console output (colored)
    logger.add(
        sys.stderr,
        format="<level>{message}</level>",
        level="DEBUG",
        enqueue=True
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
        root_dir: str | None = os.getenv("GF_DOWNLOADDIR")

        if root_dir and os.path.exists(root_dir):
            os.chdir(root_dir)

        self._lock: Lock = threading.Lock()
        self._max_workers: int = max_workers
        token: str | None = os.getenv("GF_TOKEN")
        self._message: str = " "
        self._content_dir: str | None = None

        # Keeps track of the number of recursion to get to the file
        self._recursive_files_index: int = 0

        # Dictionary to hold information about file and its directories structure
        # {"index": {"path": "", "filename": "", "link": ""}}
        # where the largest index is the top most file
        self._files_info: dict[str, dict[str, str]] = {}

        self._root_dir: str = root_dir if root_dir else os.getcwd()
        self._token: str = token if token else self._get_token()

        self._parse_url_or_file(url, password)


    def _threaded_downloads(self) -> None:
        """
        _threaded_downloads

        Parallelize the downloads.

        :return:
        """

        if not self._content_dir:
            logger.error(f"Content directory wasn't created, nothing done.")
            return

        os.chdir(self._content_dir)

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            for item in self._files_info.values():
                executor.submit(self._download_content, item)

        os.chdir(self._root_dir)


    def _create_dir(self, dirname: str) -> None:
        """
        _create_dir

        creates a directory where the files will be saved if doesn't exist and change to it.

        :param dirname: name of the directory to be created.
        :return:
        """

        current_dir: str = os.getcwd()
        filepath: str = os.path.join(current_dir, dirname)

        try:
            os.mkdir(os.path.join(filepath))
        # if the directory already exist is safe to do nothing
        except FileExistsError:
            pass


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


    def _download_content(self, file_info: dict[str, str], chunk_size: int = 16384) -> None:
        """
        _download_content

        Requests the contents of the file and writes it.

        :param file_info: a dictionary with information about a file to be downloaded.
        :param chunk_size: the number of bytes it should read into memory.
        :return:
        """

        filepath: str = os.path.join(file_info["path"], file_info["filename"])
        if os.path.exists(filepath):
            if os.path.getsize(filepath) > 0:
                logger.info(f"{filepath} already exist, skipping.")
                return

        tmp_file: str = f"{filepath}.part"
        url: str = file_info["link"]
        user_agent: str | None = os.getenv("GF_USERAGENT")

        headers: dict[str, str] = {
            "Cookie": f"accountToken={self._token}",
            "Accept-Encoding": "gzip, deflate, br",
            "User-Agent": user_agent if user_agent else "Mozilla/5.0",
            "Accept": "*/*",
            "Referer": f"{url}{('/' if not url.endswith('/') else '')}",
            "Origin": url,
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache"
        }

        part_size: int = 0
        if os.path.isfile(tmp_file):
            part_size = int(os.path.getsize(tmp_file))
            headers["Range"] = f"bytes={part_size}-"

        has_size: str | None = None
        status_code: int | None = None

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                with requests.get(url, headers=headers, stream=True, timeout=(20, 60)) as response_handler:
                    status_code = response_handler.status_code

                    if ((status_code in (403, 404, 405, 500)) or
                        (part_size == 0 and status_code != 200) or
                        (part_size > 0 and status_code != 206)):
                        logger.error(
                            f"Attempt {attempt}: Couldn't download the file from {url}."
                            f"\nStatus code: {status_code}"
                        )
                        continue

                    content_length: str | None = response_handler.headers.get("Content-Length")
                    content_range: str | None = response_handler.headers.get("Content-Range")
                    has_size = content_length if part_size == 0 \
                        else content_range.split("/")[-1] if content_range else None

                    if not has_size:
                        logger.error(
                            f"Attempt {attempt}: Couldn't find the file size from {url}."
                            f"\nStatus code: {status_code}"
                        )
                        continue

                    with open(tmp_file, "ab") as handler:
                        total_size: float = float(has_size)

                        start_time: float = time.perf_counter()
                        for i, chunk in enumerate(response_handler.iter_content(chunk_size=chunk_size)):
                            progress: float = (part_size + (i * len(chunk))) / total_size * 100

                            handler.write(chunk)

                            rate: float = (i * len(chunk)) / (time.perf_counter()-start_time)
                            unit: str = "B/s"
                            if rate < (1024):
                                unit = "B/s"
                            elif rate < (1024*1024):
                                rate /= 1024
                                unit = "KB/s"
                            elif rate < (1024*1024*1024):
                                rate /= (1024 * 1024)
                                unit = "MB/s"
                            elif rate < (1024*1024*1024*1024):
                                rate /= (1024 * 1024 * 1024)
                                unit = "GB/s"

                            with self._lock:
                                self._message = (
                                    f"Downloading {file_info['filename']}: "
                                    f"{part_size + i * len(chunk)} of {has_size} "
                                    f"{round(progress, 1)}% {round(rate, 1)}{unit} "
                                )
                                print(" " * shutil.get_terminal_size().columns, end="\r")
                                print(self._message, end="\r", flush=True)

                    
                    if has_size and os.path.getsize(tmp_file) == int(has_size):
                        print(" " * shutil.get_terminal_size().columns, end="\r")
                        logger.info(
                            f"Downloading {file_info['filename']}: "
                            f"{os.path.getsize(tmp_file)} of {has_size} Done!"
                        )
                        shutil.move(tmp_file, filepath)
                        return
            except Exception as e:
                logger.warning(f"Attempt {attempt}: Error downloading {file_info['filename']}: {e}")

            if attempt < max_retries:
                wait_time = 2 ** attempt  # wait 1, 2, 4, 8, 16, 32...
                logger.warning(f"Retrying ({attempt}/{max_retries})...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to download {file_info['filename']} after {max_retries} attempts.")


    def _parse_links_recursively(
        self,
        content_id: str,
        password: str | None = None
    ) -> None:
        """
        _parse_links_recursively

        Parses for possible links recursively and populate a list with file's info
        while also creating directories and subdirectories.

        :param content_id: url to the content.
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

        response: dict[Any, Any] = requests.get(url, headers=headers).json()

        if response["status"] != "ok":
            logger.error(f"Failed to get a link as response from the {url}.")
            return

        data: dict[Any, Any] = response["data"]

        if "password" in data and "passwordStatus" in data and data["passwordStatus"] != "passwordOk":
            logger.warning(f"Password protected link. Please provide the password.")
            return

        if data["type"] == "folder":
            # Do not use the default root directory named "root" created by gofile,
            # the naming may clash if another url link uses the same "root" name.
            # And if the root directory isn't named as the content id
            # create such a directory before proceeding
            if not self._content_dir and data["name"] != content_id:
                self._content_dir = os.path.join(self._root_dir, content_id)
                self._create_dir(self._content_dir)

                os.chdir(self._content_dir)
            elif not self._content_dir and data["name"] == content_id:
                self._content_dir = os.path.join(self._root_dir, content_id)
                self._create_dir(self._content_dir)

            self._create_dir(data["name"])
            os.chdir(data["name"])

            for child_id in data["children"]:
                child: dict[Any, Any] = data["children"][child_id]

                if child["type"] == "folder":
                    self._parse_links_recursively(child["id"], password)
                else:
                    self._recursive_files_index += 1

                    self._files_info[str(self._recursive_files_index)] = {
                        "path": os.getcwd(),
                        "filename": child["name"],
                        "link": child["link"],
                        "id": child["id"]
                    }
            os.chdir(os.path.pardir)
        else:
            self._recursive_files_index += 1
            self._files_info[str(self._recursive_files_index)] = {
                "path": os.getcwd(),
                "filename": data["name"],
                "link": data["link"],
                "id": data["id"]
            }

        # Count the frequency of each filename
        filename_count: dict[str, int] = {}
        for item in self._files_info.values():
            filename_count[item["filename"]] = filename_count.get(item["filename"], 0) + 1

        # Append the file ID to the filename only if the filename is duplicated
        for item in self._files_info.values():
            if filename_count[item["filename"]] > 1:
                filename_parts = item["filename"].rsplit('.', 1)
                filename = f"{filename_parts[0]} ({item['id'][:8]})"
                if len(filename_parts) > 1:
                    filename += f".{filename_parts[1]}"
                item["filename"] = filename


    def _print_list_files(self) -> None:
        """
        _print_list_files

        Helper function to display a list of all files for selection.

        :return:
        """

        MAX_FILENAME_CHARACTERS: int = 100
        width: int = max(len(f"[{v}] -> ") for v in self._files_info.keys())

        for (k, v) in self._files_info.items():
            # Trim the filepath if it's too long
            filepath: str = os.path.join(v["path"], v["filename"])
            filepath = f"...{filepath[-MAX_FILENAME_CHARACTERS:]}" \
                if len(filepath) > MAX_FILENAME_CHARACTERS \
                else filepath

            text: str =  f"{f'[{k}] -> '.ljust(width)}{filepath}"
            logger.info(f"{text}\n{'-' * len(text)}")


    def _download(self, url: str, password: str | None = None) -> None:
        """
        _download

        Requests to start downloading files.

        :param url: url of the content.
        :param password: content's password.
        :return:
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

        self._parse_links_recursively(content_id, _password)

        # probably the link is broken so the content dir wasn't even created.
        if not self._content_dir:
            logger.error(f"No content directory created for url: {url}, nothing done.")
            self._reset_class_properties()
            return

        # removes the root content directory if there's no file or subdirectory
        if not os.listdir(self._content_dir) and not self._files_info:
            logger.error(f"Empty directory for url: {url}, nothing done.")
            os.rmdir(self._content_dir)
            self._reset_class_properties()
            return

        interactive: bool = os.getenv("GF_INTERACTIVE") == "1"

        if interactive:
            self._print_list_files()

            input_list: list[str] = input(
                f"Files to download (Ex: 1 3 7 | or leave empty to download them all)\n:: "
            ).split()
            input_list = list(set(input_list) & set(self._files_info.keys())) # ensure only valid index strings are stored

            if not input_list:
                logger.info(f"Nothing done.")
                os.rmdir(self._content_dir)
                self._reset_class_properties()
                return

            keys_to_delete: list[str] = list(set(self._files_info.keys()) - set(input_list))

            for key in keys_to_delete:
                del self._files_info[key]

        self._threaded_downloads()
        logger.info(f"Download Completed!")
        self._reset_class_properties()


    def _parse_url_or_file(self, url_or_file: str, _password: str | None = None) -> None:
        """
        _parse_url_or_file

        Parses a file or a url for possible links.

        :param url_or_file: a filename with urls to be downloaded or a single url.
        :param password: password to be used across all links, if not provided a per link password may be used.
        :return:
        """

        if not (os.path.exists(url_or_file) and os.path.isfile(url_or_file)):
            self._download(url_or_file, _password)
            return

        with open(url_or_file, "r") as f:
            lines: list[str] = f.readlines()

        for line in lines:
            line_splitted: list[str] = line.split(" ")
            url: str = line_splitted[0].strip()
            password: str | None = _password if _password else line_splitted[1].strip() \
                if len(line_splitted) > 1 else _password

            self._download(url, password)


    def _reset_class_properties(self) -> None:
        """
        _reset_class_properties

        Simply put the properties of the class to be used again for another link if necessary.
        This should be called after all jobs related to a link is done.

        :return:
        """

        self._message: str = " "
        self._content_dir: str | None = None
        self._recursive_files_index: int = 0
        self._files_info.clear()


if __name__ == "__main__":
    try:
        setup_logger()
        
        url: str | None = None
        password: str | None = None
        argc: int = len(sys.argv)

        if argc > 1:
            url = sys.argv[1]

            if argc > 2:
                password = sys.argv[2]

            # Run
            logger.info(f"Starting, please wait...")
            Main(url=url, password=password)
        else:
            logger.info(f"Usage:\n"
                f"python gofile-downloader.py https://gofile.io/d/contentid\n"
                f"python gofile-downloader.py https://gofile.io/d/contentid password\n"
            )
            
            sys.exit(-1)
    except KeyboardInterrupt:
        sys.exit(1)