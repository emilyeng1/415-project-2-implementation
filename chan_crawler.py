from chan_client import ChanClient
import logging
from pyfaktory import Client, Consumer, Job, Producer
import datetime
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from dotenv import load_dotenv
import os
import time
import requests


# Register psycopg2 adapter for JSON data insertion
register_adapter(dict, Json)

# Load environment variables from .env file
load_dotenv()

FAKTORY_SERVER_URL = os.environ.get("FAKTORY_SERVER_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
MODERATE_HATESPEECH_API_KEY = os.environ.get("MODERATE_HATESPEECH_API_KEY")

KEYWORDS = os.environ.get("KEYWORDS", "climate change,global warming,climate crisis").split(",")
# Define keywords to filter threads
#KEYWORDS = ["climate change", "global warming", "climate crisis"]

# Logger setup
logger = logging.getLogger("4chan client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)


fh = logging.FileHandler("crawler.log")
fh.setLevel(logging.DEBUG)  # or change to INFO based on what you want to capture
fh.setFormatter(formatter)
logger.addHandler(fh)


"""
Return all the thread numbers from a catalog json object
"""
def thread_numbers_from_catalog(catalog):
    thread_numbers = []
    for page in catalog:
        for thread in page["threads"]:
            thread_number = thread["no"]
            thread_subject = thread.get("sub", "")


            # Check only the 'sub' (subject) field for keywords
            if any(keyword.lower() in thread_subject.lower() for keyword in KEYWORDS):
                thread_numbers.append(thread_number)


    return thread_numbers

"""
Return thread numbers that existed in previous but don't exist in current
"""


def get_toxicity_score(text):
    API_URL = "https://api.moderatehatespeech.com/api/v1/moderate/"
    payload = {
        "token": os.environ.get("MODERATE_HATESPEECH_API_KEY"),
        "text": text,
    }
    try:
        response = requests.post(API_URL, json=payload)
        logger.debug(f"API Request Payload: {payload}")
        logger.debug(f"API Response: {response.status_code} - {response.text}")
        response_data = response.json()


        # Log and return the "class" field or default to "normal"
        toxicity_class = response_data.get("class", "normal")
        logger.debug(f"Toxicity Class from API: {toxicity_class}")
        return toxicity_class
    except ValueError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        return "normal"
    except Exception as e:
        logger.error(f"Failed to fetch toxicity score: {e}")
        return "normal"


"""
Crawl a given thread and get its json. Insert the posts into db
"""
def crawl_thread(board, thread_number):
    chan_client = ChanClient()
    thread_data = chan_client.get_thread(board, thread_number)

    if not thread_data:
        logger.error(f"Failed to retrieve thread data for: {board}/{thread_number}")
        return

    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    for post in thread_data.get("posts", []):
        post_number = post["no"]
        post_content = post.get("com", "")


        if not post_content:
            logger.info(f"Skipping empty post content for post {post_number}")
            continue


        toxicity_score = get_toxicity_score(post_content)
        post["toxicity_score"] = toxicity_score  


        logger.debug(f"Post Content: {post_content[:50]}... | Toxicity: {toxicity_score}")

        # Check if post already exists in the database
        cur.execute(
            "SELECT 1 FROM posts WHERE board = %s AND thread_number = %s AND post_number = %s",
            (board, thread_number, post_number),
        )
        if cur.fetchone():
            logger.info(f"Post already exists: {board}/{thread_number}/{post_number}")
            continue


        try:
            query = """
            INSERT INTO posts (board, thread_number, post_number, data, toxicity_score)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
            """
            cur.execute(query, (board, thread_number, post_number, Json(post), toxicity_score))
            conn.commit()
            db_id = cur.fetchone()[0]
            logger.info(f"Inserted DB id: {db_id} for thread: {board}/{thread_number}")
        except Exception as e:
            logger.error(f"Error inserting into database: {e}")


    cur.close()
    conn.close()

"""
Go out, grab the catalog for a given board, and figure out what threads we need to collect.
For each thread to collect, enqueue a new job to crawl the thread.
Schedule catalog crawl to run again at some point in the future.
"""
def crawl_catalog(board, previous_catalog_thread_numbers=[]):
    chan_client = ChanClient()


    current_catalog = chan_client.get_catalog(board)


    if not current_catalog:
        logger.error(f"Failed to fetch catalog for board: {board}")
        return

    # Get thread numbers that match the keywords
    matching_thread_numbers = []
    for page in current_catalog:
        for thread in page["threads"]:
            thread_number = thread["no"]
            thread_subject = thread.get("sub", "")


            if thread_subject:
                toxicity_score = get_toxicity_score(thread_subject)
                logger.debug(f"Thread Subject: {thread_subject[:50]}... | Toxicity: {toxicity_score}")


            if any(keyword.lower() in thread_subject.lower() for keyword in KEYWORDS):
                matching_thread_numbers.append(thread_number)


    logger.info(f"Collected threads: {matching_thread_numbers}")


    if not matching_thread_numbers:
        logger.info("No matching threads found.")
        return


    logger.info(f"Collected threads: {matching_thread_numbers}")


    # Enqueue jobs for matching threads
    crawl_thread_jobs = []
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for thread_number in matching_thread_numbers:
            job = Job(
                jobtype="crawl-thread", args=(board, thread_number), queue="crawl-thread"
            )
            crawl_thread_jobs.append(job)


        if crawl_thread_jobs:
            producer.push_bulk(crawl_thread_jobs)
            logger.info(f"Pushed {len(crawl_thread_jobs)} jobs to 'crawl-thread' queue.")
        else:
            logger.info("No jobs to push to 'crawl-thread'.")


    # Schedule another catalog crawl to happen in the future (was gone for some reason??)
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        run_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
        run_at = run_at.isoformat()[:-7] + "Z"
        logger.info(f"run_at = {run_at}")
        job = Job(
            jobtype="crawl-catalog",
            #args=(board, current_catalog_thread_numbers),
            args=(board, matching_thread_numbers),
            queue="crawl-catalog",
            at=str(run_at),
        )
        producer.push(job)


if __name__ == "__main__":
    import sys

    # Continuous catalog crawling and job enqueuing
    if "produce" in sys.argv:
        logger.info("Starting continuous catalog crawling and job enqueuing...")

        try:
            while True:
                # Fetch the catalog and enqueue jobs
                crawl_catalog("pol")
                time.sleep(300)  # Wait for 5 minutes before the next fetch
        except Exception as e:
            logger.error(f"Error in producer loop: {e}")
        except KeyboardInterrupt:
            logger.info("Producer stopped manually.")

    # Continuous Faktory consumer
    elif "consume" in sys.argv:
        logger.info("Starting continuous Faktory consumer...")

        with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
            consumer = Consumer(
                client=client,
                queues=["crawl-thread"],
                concurrency=5  # Adjust concurrency as needed
            )
            consumer.register("crawl-thread", crawl_thread)

            try:
                consumer.run()  # runs continuously
            except KeyboardInterrupt:
                logger.info("Consumer stopped manually.")

    else:
        print("Usage: python chan_crawler.py [produce|consume]")



