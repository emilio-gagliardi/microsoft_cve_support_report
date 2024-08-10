"""kedro workbench
"""

# import os
# from pathlib import Path
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service as ChromeService
# from webdriver_manager.chrome import ChromeDriverManager
import warnings
from kedro.io.core import KedroDeprecationWarning

warnings.filterwarnings("ignore", category=KedroDeprecationWarning)
warnings.simplefilter("ignore", KedroDeprecationWarning)

__version__ = "0.1"
# options = webdriver.ChromeOptions()
# options.add_argument("--headless")
# options.add_argument("--disable-gpu")
# options.add_argument("--no-sandbox")

# try:
#     # Ensure the correct executable path
#     print("project init attempting to set chrome driver")
#     chrome_driver_path = ChromeDriverManager().install()
#     chrome_driver_executable = str(Path(chrome_driver_path).parent / "chromedriver.exe")

#     if not os.path.isfile(chrome_driver_executable):
#         raise FileNotFoundError(
#             f"ChromeDriver executable not found at {chrome_driver_executable}"
#         )

#     print(f"ChromeDriver executable path: {chrome_driver_executable}")
#     chrome_service = ChromeService(executable_path=chrome_driver_executable)
#     # chrome_browser = webdriver.Chrome(service=chrome_service, options=options)
# except Exception as e:
#     print(f"Error initializing ChromeDriver: {e}")
#     raise
