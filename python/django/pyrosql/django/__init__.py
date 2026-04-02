"""PyroSQL Django database backend.

Configure in settings.py::

    DATABASES = {
        'default': {
            'ENGINE': 'pyrosql.django',
            'HOST': 'localhost',
            'PORT': '12520',
            'NAME': 'mydb',
            'USER': 'pyrosql',
            'PASSWORD': 'secret',
        }
    }
"""
