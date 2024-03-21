FRUIT=$1
if [ $FRUIT == APPLE ]; then
	echo "You selected Apple!"
elif [ $FRUIT == Orange ]; then
	echo "You selected Orange!"
elif [ $FRUIT == Grape ]; then
	echo "You selected Grape!"
else
	echo "You selected other Fruite!"
fi
