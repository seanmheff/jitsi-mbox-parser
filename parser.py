import email.utils as email_utils
import mailbox
import requests
import bs4
import os.path
import db_wrapper
import time
import re


name_regex = re.compile('.*\((?P<name>.*?)\)')
email_regex = re.compile('(?P<username>.*) at (?P<domain>.*?) .*')


def parse_name(from_string):
    result = name_regex.search(from_string)
    return result.group('name')

def parse_email(from_string):
    result = email_regex.search(from_string)
    return result.group('username') + '@' + result.group('domain')

def extract_date(email):
    date = email.get('Date')
    return email_utils.parsedate(date)

def sort_mailbox(mailbox):
    sorted_mails = sorted(mailbox, key=extract_date)
    mailbox.update(enumerate(sorted_mails))
    mailbox.flush()
    return mailbox


# Download "http://lists.jitsi.org/pipermail/users/" and parse out GZ'd archives
response = requests.get('http://lists.jitsi.org/pipermail/users/')
parsed = bs4.BeautifulSoup(response.text, 'html.parser')
links = parsed.select('tr a')

# Want links that end in txt.gz
links = filter(lambda link: link['href'].endswith('.txt.gz'), links)
links = map(lambda link: link['href'][:-3], links)

# Convert the generator to a list
links = list(links)

# The links are in reverse sorted order - fix this
links = links[::-1]

# Loop over existing archives (txt files on disk);
for link in links:
    print('handling link ' + str(link))

    # If not present (TODO:or if current month):
    if not os.path.exists('/tmp/' + str(link)):
    
        # Download gz archive
        req = requests.get('http://lists.jitsi.org/pipermail/users/' + str(link), stream=True)

        # Extract to text file
        with open('/tmp/' + str(link), 'w+b') as f:
            for chunk in req.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

    # Read the data into a python mailbox
    monthly_mailbox = mailbox.mbox('/tmp/' + str(link))

    # Sort the mailbox so that it orders message from old -> new
    sort_mailbox(monthly_mailbox)

    # Iterate over the mailbox messages
    for email in monthly_mailbox.values():
        print(email.get('Date'))

        # Create a Person
        name = parse_name(email.get('From'))
        email_addr = parse_email(email.get('From'))
        db_wrapper.create_person(name, email_addr)

        # Get some data that we need later
        message_id = email.get('Message-ID')
        date = email_utils.parsedate_to_datetime(email.get('Date'))
        in_reply_to = email.get('In-Reply-To')
        content = email.get_payload()

        # Check if parent message in DB
        if in_reply_to:
            parent_email = db_wrapper.get_message_by_id(in_reply_to)
        else:
            parent_email = None


        if parent_email is None:
            # Create a new Thread
            # print('No parent email. Creating new thread')
            title = email.get('Subject')
            person_id = email_addr
            db_wrapper.create_thread(message_id, title, person_id, date)

            # Create a message
            thread_id = message_id
            db_wrapper.create_message(message_id, date, in_reply_to, content, person_id, thread_id)
        else:
            # Don't create a new Thread - use parent_emails thread_id
            # print('Parent email. Not creating new thread')
            thread_id = parent_email[-1] # Last column in tuple
            db_wrapper.create_message(message_id, date, in_reply_to, content, person_id, thread_id)
