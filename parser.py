import email.utils as email_utils
import mailbox
import requests
import bs4
import os.path
import db_wrapper
import time
import re
import pytz


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
    tmp = email_utils.parsedate_to_datetime(date)
    if tmp.tzinfo: # Has a utc-offset timzone (not naive)
        return tmp.astimezone(pytz.utc)
    else:
        tmp = tmp.replace(tzinfo=pytz.UTC)
        return tmp.astimezone(pytz.utc)

def sort_mailbox(joe):
    sorted_mails = sorted(joe, key=extract_date)
    joe.update(enumerate(sorted_mails))
    joe.flush()
    return joe


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

    # print(len(monthly_mailbox))
    # There is a bug in mailbox that can cause issues parsing if 
    # the email contains a line that starts with the string: "From" 
    # monthly_mailbox = list(filter(lambda email: email.get('Date') is not None, monthly_mailbox)
    for key in monthly_mailbox.keys():
        msg = monthly_mailbox.get(key)
        if msg.get('Date') is None:
            print('fucking hell')
            print(msg)
            monthly_mailbox.remove(key)
            monthly_mailbox.flush()

    # print(len(monthly_mailbox))
    # Sort the mailbox so that it orders message from old -> new
    sort_mailbox(monthly_mailbox)

    # Iterate over the mailbox messages
    for email in monthly_mailbox.values():
        print(email.get('Date'))

        # Create a Person
        try:
            name = parse_name(email.get('From'))
        except TypeError as e:
            print('error parsing email: ' + str(email))
            continue

        email_addr = parse_email(email.get('From'))
        db_wrapper.create_person(name, email_addr)

        # Get some data that we need later
        message_id = email.get('Message-ID')
        subject = email.get('Subject')
        date = email_utils.parsedate_to_datetime(email.get('Date'))
        in_reply_to = email.get('In-Reply-To')
        content = email.get_payload()

        # Check for a existing thread with the name
        existing_thread = db_wrapper.get_thread(subject)


        if existing_thread is None:
            # Create a new Thread
            # print('No parent email. Creating new thread')
            db_wrapper.create_thread(message_id, subject, email_addr, date)

            # Create a message
            db_wrapper.create_message(message_id, date, in_reply_to, content, email_addr, message_id)
        else:
            # Don't create a new Thread - use parent_emails thread_id
            # print('Parent email. Not creating new thread')
            thread_id = existing_thread[0] # Last column in tuple
            db_wrapper.create_message(message_id, date, in_reply_to, content, email_addr, thread_id)

    monthly_mailbox.close()
