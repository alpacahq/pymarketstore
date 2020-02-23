import time
import numpy as np
from pymarketstore import Client, DataShape, DataShapes

cli = Client()

o = DataShape(name='Open', typ='float64')
h = DataShape(name='High', typ='float64')
l = DataShape(name='Low', typ='float64')
c = DataShape(name='Close', typ='float64')
v = DataShape(name='Volume', typ='int64')
e = DataShape(name='Epoch', typ='int64')

shapes = DataShapes()
shapes.add(o)
shapes.add(h)
shapes.add(l)
shapes.add(c)
shapes.add(v)
shapes.add(e)

cli.create('TSLA/15Min/OHLCV', shapes)

data = np.array([(int(time.time()), 0, 1, 1, 1, 1)],
                dtype=[('Epoch', 'i8'), ('Open', 'f8'), ('Close', 'f8'),
                       ('High', 'f8'), ('Low', 'f8'), ('Volume', 'i8')])
cli.write(data, 'TSLA/15Min/OHLCV')
