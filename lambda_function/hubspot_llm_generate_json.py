from typing import Dict, Any
from dotenv import load_dotenv
from datetime import datetime
import json, re
from tools import *

# =========================================
# Fonction Lambda
# =========================================
def lambda_handler(event, context):
    """
    Lambda pour extraire des données via LLM depuis le dernier fichier OCR
    et mettre à jour le log existant correspondant au PDF.
    """

    # ----------------------------------------------------------->
    # (1) Définir le bucket S3
    bucket = "hubspot-tickets-pdf"

    # (2) Connexion AWS
    aws_conn = connexion_aws()
    if aws_conn["status"] != "success":
        return {"statusCode": 500, "body": json.dumps({"error": "Connexion AWS échouée"})}
    s3_client = aws_conn["client"]
    # ----------------------------------------------------------->

    try:
        # ----------------------------------------------------------->
        # (3) Récupérer le dernier fichier OCR
        s3_key = get_last_file_ocr(s3_client, bucket, prefix="PDF_OCR/")
        print(f"📄 Dernier fichier trouvé : {s3_key}")
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (4) Extraire le nom du fichier PDF depuis le nom du fichier OCR
        match = re.search(r"\[(.*?)\]", s3_key)
        if match:
            name_file = match.group(1)
            print(f"Nom du fichier PDF : {name_file}")
        else:
            raise ValueError("Impossible d'extraire le nom du PDF depuis le nom du fichier OCR")
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (5) Récupérer le log JSON existant pour ce PDF
        log_key  = f"LOGS/log_[{name_file}].json"
        log_obj  = s3_client.get_object(Bucket=bucket, Key=log_key)
        log_data = json.loads(log_obj["Body"].read())
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (6) Définir le key de sortie pour le JSON DEAL
        output_key = f"DEAL_JSON/DEAL_[{name_file}].json"
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (7) Extraction JSON via LLM
        extracted_json = extract_data_from_s3_pdf_ocr(
            s3_client=s3_client,
            bucket_name=bucket,
            s3_key=s3_key
        )
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (8) Mise à jour du log avec succès
        log_data["workflow"]["LLM"]["status"]  = "Success"
        log_data["workflow"]["LLM"]["details"] = f"Extraction completed"
        log_data["workflow"]["LLM"]["data"]    = extracted_json
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (9) Sauvegarde du JSON DEAL dans S3
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=json.dumps(extracted_json, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        print(f"✅ JSON sauvegardé dans S3 ({output_key})")
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (10) Mise à jour du log S3
        s3_client.put_object(
            Bucket=bucket,
            Key=log_key,
            Body=json.dumps(log_data, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        print(f"✅ Log mis à jour dans S3 ({log_key})")
        # ----------------------------------------------------------->

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Extraction et sauvegarde réussies",
                "output_key": output_key,
                "log_key": log_key
            })
        }

    except Exception as e:
        
        # ----------------------------------------------------------->
        # (11) Gestion d'erreur et mise à jour log existant uniquement
        print(f"Erreur dans Lambda : {e}")
        if 'log_data' in locals():
            log_data["workflow"]["LLM"]["status"] = "Failed"
            log_data["workflow"]["LLM"]["details"] = str(e)
            s3_client.put_object(
                Bucket=bucket,
                Key=log_key,
                Body=json.dumps(log_data, ensure_ascii=False, indent=2),
                ContentType="application/json"
            )
            print(f"⚠️ Log mis à jour avec l'erreur ({log_key})")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
        # ----------------------------------------------------------->
        
        
        
        