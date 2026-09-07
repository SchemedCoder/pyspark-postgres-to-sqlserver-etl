import yaml
from dotenv import load_dotenv
from src.etl_job import BurgerCourierETL
from src.logger import get_logger

logger = get_logger("ControlTower")

def main():
    # 1. Unlock the briefcase (Load environment variables)
    load_dotenv()
    
    # 2. Read the flight manifest (Load YAML config)
    logger.info("Reading the flight manifest...")
    with open('config/settings.yaml', 'r') as file:
        config = yaml.safe_load(file)
        
    # 3. Take off!
    courier = BurgerCourierETL(config)
    courier.run()

if __name__ == "__main__":
    main()
