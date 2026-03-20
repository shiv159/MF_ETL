import mstarpy
from selenium.webdriver.chrome.options import Options

original_add_argument = Options.add_argument

def patched_add_argument(self, argument):
    original_add_argument(self, argument)
    if argument == "--headless=new" or argument == "--headless":
        original_add_argument(self, "--no-sandbox")
        original_add_argument(self, "--disable-dev-shm-usage")

Options.add_argument = patched_add_argument

f = mstarpy.Funds('F00000PDD2')
print("INIT WORKED")
