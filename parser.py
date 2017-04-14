import mailbox
import requests
import bs4
import os.path


# Download "http://lists.jitsi.org/pipermail/users/" and parse out GZ'd archives
response = requests.get('http://lists.jitsi.org/pipermail/users/')
parsed = bs4.BeautifulSoup(response.text, 'html.parser')
links = parsed.select('tr a')

# Want links that end in txt.gz
links = filter(lambda link: link['href'].endswith('.txt.gz'), links)
links = map(lambda link: link['href'][:-3], links)

# Loop over existing archives (txt files on disk);
for link in links:

    # If not present (TODO:or if current month):
    if not os.path.isfile('mboxs/' + str(link)):
    
        # Download gz archive
        # Extract to text file
        req = requests.get('http://lists.jitsi.org/pipermail/users/' + str(link), stream=True)
        with open('mboxs/' + str(link), 'wb') as f:
            for chunk in req.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

    # Starting at the oldest file, read the data into a python mailbox

    # Loop through the data
    # Create new threads in the DB for non-reply messages - these are new threads
    # Pop these thread starters out of the list - done processing with them


    # While messages remain in the list:
        # Keep popping off 


'''
emails = mailbox.mbox('./2017-April.txt')

email = emails.items()[2][1]
print email
print [method for method in dir(email) if callable(getattr(email, method))]

for email in emails:
    if email.get('In-Reply-To') is None:
        print "start of thread" + str(email.get('Subject'))

'''
