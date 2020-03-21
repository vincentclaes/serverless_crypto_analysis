import csv
import os
import json
import boto3
import botocore
import datetime

from retrying import retry

# init clients
athena = boto3.client("athena")
s3 = boto3.resource("s3")
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    athena_db = os.environ["ATHENA_DB"]
    print(athena_db)
    athena_table = os.environ["ATHENA_TABLE"]
    # athena_table = "prd_coinmarketcap_data"
    print(athena_table)
    bucket_data = os.environ["BUCKET_DATA"]
    print(bucket_data)
    key_data = os.environ["KEY_DATA"]
    print(key_data)
    rank_list = json.loads(os.environ["RANK"])
    print(rank_list)
    uuid = event.get("uuid")
    if uuid is None:
        uuid = get_max_uuid(athena_db, athena_table)
    ret_val = {}
    for rank in rank_list:
        print("running for rank : {}".format(rank))
        newcomers = get_newcomers_for_rank(
            athena_db, athena_table, bucket_data, key_data, uuid, rank
        )
        ret_val[rank] = newcomers
    return ret_val


def get_newcomers_for_rank(
    athena_db, athena_table, bucket_data, key_data, max_uuid, rank
):
    latest_results = get_latest_result(athena_db, athena_table, max_uuid, rank)
    tail_results = get_tail_results(athena_db, athena_table, max_uuid, rank)
    newcomers = get_newcomers(latest_results, tail_results)
    dump_newcomers(newcomers, bucket_data, key_data, rank)


def get_latest_results_for_rank(latest_results, rank):
    df_latest_for_rank = latest_results[latest_results["rank"].astype(int) <= rank]
    return df_latest_for_rank


def dump_newcomers(newcomers, bucket, dest_key, rank):
    date_time = datetime.datetime.utcnow().isoformat()
    for newcomer in newcomers:
        full_dest_key = os.path.join(
            dest_key, "{}-{}-{}.json".format(date_time, rank, newcomer)
        )
        object_ = {
            "id":newcomer,
            "rank":rank
        }
        boto3.client("s3").put_object(Body=json.dumps(object_), Bucket=bucket, Key=full_dest_key)


def get_newcomers(latest_results, tail_results):
    ret_val = []
    tail_results = set(tail_results)
    for latest_element in latest_results:
        newcomer = latest_element not in tail_results
        if newcomer:
            print("newcomer found : {}".format(newcomer))
            ret_val.append(latest_element)
    return ret_val


def get_max_uuid(athena_db, athena_table):
    result = run_query(
        """SELECT max(uuid) FROM "{}"."{}";""".format(athena_db, athena_table),
        athena_db,
    )
    print("max uuid result {}".format(result))
    max_uuid = result[0]["_col0"]
    return max_uuid


def get_latest_result(athena_db, athena_table, max_uuid, rank):
    results = run_query(
        """select id from "{}"."{}" where uuid={} and rank<{};""".format(
            athena_db, athena_table, max_uuid, rank
        ),
        athena_db,
    )
    print("latest result {}".format(results))
    latest_results = get_ids_from_results(results)
    return latest_results


def get_tail_results(athena_db, athena_table, max_uuid, rank):
    results = run_query(
        """select distinct id from "{}"."{}" where uuid<{} and rank <= {};""".format(
            athena_db, athena_table, max_uuid, rank
        ),
        athena_db,
    )
    print("tail result {}".format(results))
    tail_results = get_ids_from_results(results)
    return tail_results


def get_ids_from_results(results):
    return [result.get("id") for result in results]


def run_query(query, athena_db):
    return _run_query(
        query,
        athena_db,
        "s3://aws-athena-query-results-077590795309-eu-central-1",
        "aws-athena-query-results-077590795309-eu-central-1",
    )


@retry(
    stop_max_attempt_number=10,
    wait_exponential_multiplier=300,
    wait_exponential_max=1 * 60 * 1000,
)
def poll_status(_id):
    result = athena.get_query_execution(QueryExecutionId=_id)
    state = result["QueryExecution"]["Status"]["State"]
    print("poll status : {}".format(state))
    if state == "SUCCEEDED":
        return result
    elif state == "FAILED":
        return result
    else:
        raise Exception


def _run_query(query, database, s3_output, output_bucket):
    print("Executing query: {}".format(query))
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": s3_output,},
    )

    QueryExecutionId = response["QueryExecutionId"]
    result = poll_status(QueryExecutionId)
    print("final poll status")
    if result["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
        print("Query SUCCEEDED: {}".format(QueryExecutionId))

        s3_key = QueryExecutionId + ".csv"
        local_filename = QueryExecutionId + ".csv"

        # download result file
        try:
            response = s3_client.get_object(Bucket=output_bucket, Key=s3_key)
            lines = response["Body"].read().decode("utf-8").split()

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print("The object does not exist.")
            else:
                raise

        # read file to array
        rows = []
        reader = csv.DictReader(lines)
        for row in reader:
            rows.append(row)

        # delete result file
        if os.path.isfile(local_filename):
            os.remove(local_filename)

        print("result: {}".format(rows))
        return rows
