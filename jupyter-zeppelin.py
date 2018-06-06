import csv
import os, sys
import re
import json
import html
import nbformat
import codecs
from boto3 import s3
from io import StringIO
from pyspark.sql import SparkSession
from setuptools._vendor.pyparsing import col

MD = re.compile(r'%md\s')
SQL = re.compile(r'%sql\s')
UNKNOWN_MAGIC = re.compile(r'%\w+\s')
HTML = re.compile(r'%html\s')

def read_io(path):
    """Reads the contents of a local or S3 path into a StringIO.
    """
    note = StringIO()
    if path.startswith("s3://"):
        ss3 = s3(env='prod')
        for line in ss3.read(path):
            note.write(line)
            note.write("S3")
    else:
        with open(path) as local:
            for line in local.readlines():
                note.write(line)

    note.seek(0)

    return note

def table_cell_to_html(cell):
    """Formats a cell from a Zeppelin TABLE as HTML.
    """
    if HTML.match(cell):
        # the contents is already HTML
        return cell
    else:
        return html.escape(cell)
        # Python 2 html does not support escape
        #from xml.sax.saxutils import escape
        #return escape(cell)

def table_to_html(tsv):
    """Formats the tab-separated content of a Zeppelin TABLE as HTML.
    """
    io = StringIO(tsv)
    reader = csv.reader(io, delimiter="\t")
    fields = reader.next()
    column_headers = "".join([ "<th>" + name + "</th>" for name in fields ])
    lines = [
            "<table>",
            "<tr>{column_headers}</tr>".format(column_headers=column_headers)
        ]
    for row in reader:
        lines.append("<tr>" + "".join([ "<td>" + table_cell_to_html(cell) + "</td>" for cell in row ]) + "</tr>")
    lines.append("</table>")
    return "\n".join(lines)


def convert_json(zeppelin_json):
    """Converts a Zeppelin note from JSON to a Jupyter NotebookNode.
    """
    return convert_parsed(json.load(zeppelin_json))

def convert_parsed(zeppelin_note):
    """Converts a Zeppelin note from parsed JSON to a Jupyter NotebookNode.
    """
    notebook_name = zeppelin_note['name']

    cells = []
    index = 0
    for paragraph in zeppelin_note['paragraphs']:
        code = paragraph.get('text')
        if not code:
            continue

        code = code.lstrip()

        cell = {}

        if MD.match(code):
            cell['cell_type'] = 'markdown'
            cell['metadata'] = {}
            cell['source'] = code.lstrip('%md').lstrip("\n") # remove '%md'
        elif SQL.match(code) or HTML.match(code):
            cell['cell_type'] = 'code'
            cell['execution_count'] = index
            cell['metadata'] = {}
            cell['outputs'] = []
            cell['source'] = '%' + code # add % to convert to cell magic
        elif UNKNOWN_MAGIC.match(code):
            # use raw cells for unknown magic
            cell['cell_type'] = 'raw'
            cell['metadata'] = {'format': 'text/plain'}
            cell['source'] = code
        else:
            cell['cell_type'] = 'code'
            cell['execution_count'] = index
            cell['metadata'] = {'autoscroll': 'auto'}
            cell['outputs'] = []
            cell['source'] = code

        cells.append(cell)

        result = paragraph.get('result')
        if cell['cell_type'] == 'code' and result:
            if result['code'] == 'SUCCESS':
                result_type = result.get('type')
                output_by_mime_type = {}
                if result_type == 'TEXT':
                    output_by_mime_type['text/plain'] = result['msg']
                elif result_type == 'HTML':
                    output_by_mime_type['text/html'] = result['msg']
                elif result_type == 'TABLE':
                    output_by_mime_type['text/html'] = table_to_html(result['msg'])

                cell['outputs'] = [{
                    'output_type': 'execute_result',
                    'metadata': {},
                    'execution_count': index,
                    'data': output_by_mime_type
                }]

        index += 1

    notebook = nbformat.from_dict({
        "metadata": {
            "kernelspec": {
                "display_name": "Spark 2.0.0 - Scala 2.11",
                "language": "scala",
                "name": "spark2-scala"
            },
            "language_info": {
                "codemirror_mode": "text/x-scala",
                "file_extension": ".scala",
                "mimetype": "text/x-scala",
                "name": "scala",
                "pygments_lexer": "scala",
                "version": "2.11.8"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 2,
        "cells" : cells,
    })

    return (notebook_name, notebook)

def write_notebook(notebook_name, notebook, path=None):
    """Writes a NotebookNode to a file created from the notebook name.

    If path is None, the output path will be created the notebook name in the current directory.
    """
    filename = path
    if not filename:
        filename = notebook_name + '.ipynb'
        if os.path.exists(filename):
            for i in range(1, 1000):
                filename = notebook_name + ' (' + str(i) + ').ipynb'
                if not os.path.exists(filename):
                    break
                if i == 1000:
                    raise RuntimeError('Cannot write %s: versions 1-1000 already exist.' % (notebook_name,))

    with codecs.open(filename, 'w', encoding='UTF-8') as io:
        nbformat.write(notebook, io)

    return filename

def zeppelinToDB(zepJSON):
    import json
    from datetime import datetime

    #parse JSON from file and separate into individual paragraphs
    json_data=open(zepJSON).read().encode('ascii', 'ignore').decode('ascii')
    data = json.loads(json_data)
    paragraphs = data["paragraphs"]

    #identify interpreter type
    commentType = "// "
    extType = ".scala"
    for paragraph in paragraphs:
        if "text" in paragraph:
            if "pyspark" in paragraph["text"]:
                commentType = "# "
                extType = ".py"
                break

    #convert each paragraph into equivalent Databricks cell type
    tmstr = datetime.now().strftime("%c")
    script = commentType+"Databricks notebook source exported at " + tmstr + "\n"
    for paragraph in paragraphs:
        if not "text" in paragraph: continue
        text = paragraph.get("text")
        if  "%pyspark" in text:
            script += commentType + " COMMAND ----------\n" + text.replace("%pyspark", commentType) + "\n" + commentType + " COMMAND ----------\n"
        elif  "%sh" in text:
            script += commentType + " COMMAND ----------\n" + commentType + " MAGIC " + text + "\n" + commentType + " COMMAND ----------\n"
        elif  "%sql" in text:
            script += commentType + " COMMAND ----------\n" + commentType + " MAGIC " + text + "\n" + commentType + " COMMAND ----------\n"
        elif  "%" in text:
            lines = text.split("\n")
            for line in lines:
                script += commentType + " MAGIC " + line + "\n"
        else:
            script += "\n" + commentType + " COMMAND ----------\n" + text + "\n" + commentType + " COMMAND ----------\n"
    filename = "outfile" + extType

    return (filename, script)

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("ZeppelinConvert") \
        .getOrCreate()

    num_args = len(sys.argv)
    print(sys.argv[1])
    print(sys.argv[2])
    print(sys.argv[3])

    #zeppelin_note_path = "/Users/justinmichaels/IdeaProjects/zeppelin-notebooks/2AS5TY6AQ/note.json"
    #target_path = open("/Users/justinmichaels/IdeaProjects/jupyter-zeppelin/scalaConvert.scala", 'w')
    if sys.argv[1] == "one":
         zeppelin_note_path = sys.argv[2]
         target_path = open(sys.argv[3], 'w')
         name, content = convert_json(read_io(zeppelin_note_path))
         name, content = zeppelinToDB(zeppelin_note_path)
         write_notebook(name, content, target_path)
    elif sys.argv[1] == "two":
         zeppelin_note_path = sys.argv[2]
         target_path = open(sys.argv[3], 'w')
         databricks = zeppelinToDB(zeppelin_note_path)
         target_path.write("".join(databricks))
         target_path.close()
    else:
        print("usage: python3 jupyter-zeppelin.py <1 or 2> <path to Zeppelin notebook file> <path to target output file>")
        exit()
