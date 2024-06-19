import os
import streamlit as st
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from azure.storage.blob import BlobServiceClient
import schedule
import time
import threading
import logging
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')
print("DATABASE_URL:", DATABASE_URL)

logging.basicConfig(level=logging.INFO)

engine = create_engine(DATABASE_URL)
Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    brand = Column(String)
    image_path = Column(String)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = 'product-images'
container_client = blob_service_client.get_container_client(container_name)
def blob_exists(blob_name):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        return blob_client.exists()
    except Exception as e:
        logging.error(f"Error checking if blob exists: {e}")
        return False
def upload_image(image_path):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=image_path)
        with open(image_path, "rb") as data:
            blob_client.upload_blob(data)
        logging.info(f"Uploaded image: {image_path}")
    except Exception as e:
        logging.error(f"Error uploading image: {e}")
def parse_xml_and_update_db():
    try:
        tree = ET.parse('products.xml')
        root = tree.getroot()
        existing_product_ids = {product.id for product in session.query(Product.id).all()}

        xml_product_ids = set()
        for product in root.findall('product'):
            product_id = int(product.find('id').text)
            xml_product_ids.add(product_id)
            name = product.find('name').text
            brand = product.find('brand').text
            image_path = product.find('image').text

            product_record = session.query(Product).filter_by(id=product_id).first()
            if product_record:
                product_record.name = name
                product_record.brand = brand
                product_record.image_path = image_path
            else:
                new_product = Product(id=product_id, name=name, brand=brand, image_path=image_path)
                session.add(new_product)
            if not blob_exists(image_path):
                upload_image(image_path)
        products_to_remove = existing_product_ids - xml_product_ids
        for product_id in products_to_remove:
            session.query(Product).filter_by(id=product_id).delete()

        session.commit()
        logging.info("Database update completed")
    except Exception as e:
        logging.error(f"Error updating database: {e}")
def job():
    logging.info("Running scheduled job")
    parse_xml_and_update_db()

schedule.every().day.at("01:00").do(job)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()
st.title('Product Search')

brand = st.text_input('Enter brand name:')
if brand:
    try:
        products = session.query(Product).filter_by(brand=brand).all()
        if products:
            for product in products:
                st.write(f"ID: {product.id}")
                st.write(f"Name: {product.name}")
                st.write(f"Brand: {product.brand}")
                st.image(f"https://{container_name}.blob.core.windows.net/{product.image_path}")
        else:
            st.write("No products found for this brand")
    except Exception as e:
        st.error(f"Error retrieving products: {e}")

if __name__ == '__main__':
    st.run()
