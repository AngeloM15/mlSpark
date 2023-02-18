export version=`python --version |awk '{print $2}' |awk -F"." '{print $1$2}'`

echo $version

if [ $version == '36' ] || [ $version == '37' ]; then
    echo 'Starting installation...'
    pip3 install pyspark==2.4.8 wget==3.2 > install.log 2> install.log
    if [ $? == 0 ]; then
        echo 'Required packages installed'
    else
        echo 'Installation failed, please check log:'
        cat install.log
    fi
elif [ $version == '38' ] || [ $version == '39' ]; then
    pip3 install pyspark==3.1.2 wget==3.2 > install.log 2> install.log
    if [ $? == 0 ]; then
        echo 'Required packages installed'
    else
        echo 'Installation failed, please check log:'
        cat install.log
    fi
else
    echo 'Currently only python 3.6, 3.7 , 3.8 and 3.9 are supported'
    exit -1
fi

# Pull the data in raw format from the source (github)
SCRIPT=$(readlink -f "$0")
DATAPATH="$(dirname "$SCRIPT")/../data"

rm -Rf "$DATAPATH/HMP_Dataset"
git clone https://github.com/wchill/HMP_Dataset "$DATAPATH/HMP_Dataset"
