# Elasticstat(1) man pages

## Installation

Unfortunately, setuptools/distribute don't provide facilities for installing man pages, so you'll need to manually install the man page as follows:

``` bash
$ sudo mv elasticstat.1 /usr/local/share/man/man1
```

Alternatively, you can just view the man page by running:

``` bash
$ man ./elasticstat.1
```

## Regenerating

These man pages are generated with [ronn](https://rtomayko.github.io/ronn/). You can re-create them by [installing ronn](https://github.com/rtomayko/ronn/blob/master/INSTALLING) and running:

``` bash
$ ronn -r elasticstat.ronn
``` 
