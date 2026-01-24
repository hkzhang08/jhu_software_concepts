"""
Class:         EP 605 2256 81 - Spring 2026
Assignment:    Module_1 - Personal Website
Due Date:      January 25th by 11:59AM
Name:          Helen Zhang
Email:         hzhan308@jh.edu
"""



# Skills: Flask, HTML, Searching Methods, Data Cleaning, JSON Data Object Storage


# Import html file for home page (include name, position, bio, picture)
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')

def homepage():
    return render_template('homepage.html')

@app.route('/contact')
def contact():
    return render_template('contact.html')

@app.route('/projects')
def projects():
    return render_template('projects.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
