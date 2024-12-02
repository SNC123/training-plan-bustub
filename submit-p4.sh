cd ./build
make format
make submit-p4
cd ..
python3 gradescope_sign.py 