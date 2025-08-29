import os
import boto3
import json
from typing import Dict, Any
from dotenv import load_dotenv

from tools import *

# =========================================
# Fonction Lambda
# =========================================
def lambda_handler(event, context):
    bucket = "hubspot-tickets-pdf"

    # Connexion AWS.
    aws_conn = connexion_aws()
    if aws_conn["status"] != "success":
        return {"statusCode": 500, "body": json.dumps({"error": "Connexion AWS échouée"})}
    s3_client = aws_conn["client"]

    try:
        # Récupérer le dernier fichier OCR dans PDF_OCR/
        s3_key = get_last_file_ocr(s3_client, bucket, prefix="PDF_OCR/")
        print(f"📄 Dernier fichier trouvé : {s3_key}")
        
        # Définir un output_key basé sur le nom du fichier OCR
        output_key = "DEAL_JSON/" + s3_key.split("/")[-1].replace("OCR_", "DEAL_").replace(".txt", ".json")

        # Extraction JSON via LLM.
        extracted_json = extract_data_from_s3_pdf_ocr(s3_client=s3_client, bucket=bucket, s3_key=s3_key)

        # Sauvegarde du JSON dans S3.
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=json.dumps(extracted_json, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        print(f"✅ JSON sauvegardé dans S3 ({output_key})")
        return {"statusCode": 200, "body": json.dumps({"message": "Extraction et sauvegarde réussies", "output_key": output_key})}

    except Exception as e:
        print(f"Erreur dans Lambda : {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


