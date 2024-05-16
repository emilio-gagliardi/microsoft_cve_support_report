"""kedro workbench
"""
import warnings
from kedro.io.core import KedroDeprecationWarning
warnings.filterwarnings("ignore", category=KedroDeprecationWarning)
warnings.simplefilter("ignore", KedroDeprecationWarning)

__version__ = "0.1"
