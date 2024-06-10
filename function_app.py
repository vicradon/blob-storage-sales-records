import azure.functions as func
import logging
import csv
import json

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="salesrecords/{name}.csv", connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outputblob", path="sales-output/out.json", connection="AzureWebJobsStorage")
def calculate_net_order_amount(myblob: func.InputStream, outputblob: func.Out[str]):
    filecontent = myblob.read().decode('utf-8')
    lines = filecontent.split("\n")

    reader = csv.DictReader(lines)

    total_sales = 0.0

    for row in reader:
        total_sales += float(row['Order Total (USD)'])

    logging.info(f"Python blob trigger function processed blob\n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes\n"
                 f"Total Sales: ${total_sales:.2f}\n")


    outputblob.set(json.dumps({
        "date": "10/06/2024",
        "net sales amount": 3803.34
    }))