## Zeppelin Notebook Conversion

Simple conversion utility for converting Zeppelin notebooks to Jupyter's ipynb format. Depending on the format of the notebook the parsing may need an alternative parsing format. There are two modes this utility can run:
<br> 1. Based from https://github.com/rdblue/jupyter-zeppelin repo and provides python based notebooks.
<br> 2. Many notebooks do not parse correctly and a more crude approach might be needed. This mode parses the notebook text directly.

To convert a notebook, run:

```
usage: python3 jupyter-zeppelin.py <1 or 2> <path to Zeppelin notebook file> <path to target output file>
```

This will create a file based on the name and directory passed.

### Supported Conventions

This converter supports the following Zeppelin conventions:

* Code paragraphs are converted to code cells
* `%md` paragraphs are converted to Jupyter markdown cells
* `%html` paragraphs are converted to Jupyter code cells using cell magic `%%html`
* `%sql` paragraphs are converted to Jupyter code cells using cell magic `%%sql`
* Paragraphs with unknown magics are converted to raw cells
* TEXT output is converted to `text/plain` output
* HTML output is converted to `text/html` output; some style and JS may not work in Jupyter
* TABLE output is converted to simple `text/html` tables
  * `%html` table cells are embedded in the table HTML
  * Normal table cells are escaped and then embedded in the table HTML
