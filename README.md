# Unveil

This tool uses the Companies House API and the official list of entities sanctioned by the UK to generate a list of officers that are suspicious for sanctions avoidance.

The suspicion is measured by 7 red flags:
-   DOB Flag (date of birth matches the related entity in the sanctionslist)
-   Name Flag (First and last name are similar to the related entity in the santionslist)
-   Area Flag Officer (Officer is located in the London area)
-   Area Flag Company (Company is located in the London area)
-   Address Flag (more than five companies are registered at the companies address)
-   Country Flag (Company is related to suspicious country (see list in Data))
-   Date Flag (Change in company ownership in the first quarter 2022)

The results are displayed in a .csv file for further analysis


To run the program the following steps need to be taken:

1. Download the source and extract it to a place where you can find it.
2. Install the required packages using "pip install -r requirements.txt".
3. Go to create https://developer.company-information.service.gov.uk/get-started and create an API key
4. Download the ODF-Format Sanctions List from https://www.gov.uk/government/publications/the-uk-sanctions-list and change the heading of the Column Name 6 to Name 4. I also recommend removing all entries not related to the targeted Sanctions Regime to make loading faster. Until now only Russia has been tested. Save the modified file in the Data folder.
5. Open the program directory in a terminal and run "python Main.py"

It was tried to also use the Companies House PSC data (download.companieshouse.gov.uk/en_pscdata.html) but it has not been successfully implemented yet.