## Compiling the Documentation

Run the following command

```bash
cd docs
make html
cd _build/html/
python -m http.server 8000
```

This command compiles the documentation into HTML and serves it on port 8000.

To view the documentation point your browser to <http://localhost:8000/>.

## Writing the Documentation

To get started see <https://pythonhosted.org/an_example_pypi_project/sphinx.html>.

## Running the tests

The packages `py.test` and `pytest-ordering` need to be installed in your (virtual) environment.

To run all tests run (from the repository's root folder):

    py.test -vv

To see the standard ouput, just use the `-s` flag

    py.test -s

Currently, the tests are configured so that logging messages (`level=INFO`)
are printed to `stdout`. To change this edit the file `tests/conftest.py` accordingly.

To run all tests for a single package (e.g. `commons`):

    py.test tests/commons -vv

To run a specific test (here printing the log to `stdout`):

    py.test tests/commons/test_dbmodels.py -s


## Installing the API for production

To install the API under <http://dhlabsrv10.epfl.ch/dev/api/>:

`/etc/apache2/sites-available/lbapi.conf`

```bash
WSGIDaemonProcess lbapi_v1 user=www-data group=www-data processes=1 threads=20 maximum-requests=1000
WSGIScriptAlias /dev /var/www/venice_scholar_dev/api.wsgi

<VirtualHost *:80>

        ServerName dhlabsrv10.epfl.ch
        ServerAdmin webmaster@localhosl
	# no need to change (unused)
        DocumentRoot /var/www/html
	# no need to change (unused)

        ErrorLog ${APACHE_LOG_DIR}/error.log
        CustomLog ${APACHE_LOG_DIR}/access.log combined

        ExpiresActive On
        ExpiresDefault "access plus 5184000 seconds"

        AllowEncodedSlashes On


        <Location /dev>
		WSGIProcessGroup lbapi_v1
        </Location>

</VirtualHost>
```

`/var/www/venice_scholar_dev/api.wsgi`:

```bash
python_home = '/home/mongodbsrv/.pyenv/versions/3.5.0/envs/lbapi'

import sys
import site

# Calculate path to site-packages directory.

python_version = '.'.join(map(str, sys.version_info[:2]))
site_packages = python_home + '/lib/python%s/site-packages' % python_version

# Add the site-packages directory.

site.addsitedir(site_packages)

print(site_packages)

sys.path.insert(0, '/var/www/venice_scholar_dev/')
sys.path.append('/home/mongodbsrv/linkedbooks_code/')

from api.run_api import create_app

application = create_app(config_file='/home/mongodbsrv/linkedbooks_code/api/config/dev.cfg')
```
