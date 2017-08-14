import tensorflow as tf
hello = tf.constant(2)
sess = tf.Session()
print(sess.run(hello))