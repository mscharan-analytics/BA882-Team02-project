######
## simple script for now to deploy functions
## deploys all, which may not be necessary for unchanged resources
######

# setup the project
gcloud config set project group2-ba882

# schema setup
echo "======================================================"
echo "deploying the schema setup"
echo "======================================================"

gcloud functions deploy group2-schema-setup \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-311_Service_Requests/functions/schema-setup \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB 

# extract rss
echo "======================================================"
echo "deploying the rss extractor"
echo "======================================================"

gcloud functions deploy group2-extract-rss \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-311_Service_Requests/functions/extract-rss \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB 

# load the feeds into raw and changes into stage
echo "======================================================"
echo "deploying the loader"
echo "======================================================"

gcloud functions deploy group2-load-rss \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-311_Service_Requests/functions/load-rss \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB 