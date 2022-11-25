from setuptools import setup, find_packages


setup(
    name='nurtelecom_gras_library',
    version='0.1',
    license='MIT',
    author="Beksultan Tuleev",
    author_email='kazamabeks@gmail.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/beksultantuleev/nurtelecom_gras_library.git',
    keywords='NurTelecom',
    install_requires=[
          'scikit-learn',
          'cx_Oracle', 
          'pandas',
          'sqlalchemy',
          'shapely',
          'matplotlib'
      ],

)