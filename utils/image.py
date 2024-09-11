import hashlib
import requests
from PIL import Image
from io import BytesIO


def hash_image(image_url):

    # Fetch the image from the image URL
    response = requests.get(image_url)
    if response.status_code == 200:
        # Open the image using PIL
        img = Image.open(BytesIO(response.content))

        # Convert image to bytes
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format=img.format)
        img_bytes = img_byte_arr.getvalue()

        # Compute MD5 hash of the image content
        img_hash = hashlib.md5(img_bytes).hexdigest()

        return img_hash
