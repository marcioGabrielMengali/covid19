<h2>Covid 19 - ETL Project 😷</h2>

<h3>Description 📌</h3>
<p>
<b>💾 About the dataset:</b> the data is about the covid 19 pandemic in Brazil, bringing data from Brazilian cities and states
</p>
<p>
ETL project created to extract data from a url, going through the extraction, transformation and load phases.
Data is extracted from a url that downloads a .gz file that is decompressed and saved in data/raw_data.
After downloading using a transformation script and using a notebook for initial analysis, the transformations were defined.
The data is loaded into the Postgres database. The data was splited between state and city..
</p>


<h4>Technologies 💻</h4>
<p>
     main technologies used are Python and SQL.
</p>
<p><b>Python version = 3.11</b></p>

<h4>How to run the project 🏃</h4>
<ol>
    <li>use command: <b>>pip install -r /path/to/requirements.txt</b</li>
    <li>execute: <b>python main.py</b></li>
</ol>

<h4>Folder Structure 📁</h4>
<ul>
    <li>config = files that have project settings</li>
    <li>data = data extracted and processeds</li>
    <li>data_quality = notebook with data quality analysis on raw data/li>
    <li>etl_scripts = contains ETL scripts</li>
    <li>log = log files</li>
    <li>notebooks = notebooks used for project development</li>
</ul>

