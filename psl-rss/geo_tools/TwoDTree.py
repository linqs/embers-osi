'''
Created on Jun 23, 2010

@author: bhuang
'''

import math
import random
from haversine import points2distance

def distance(p1, p2):
    return math.sqrt(math.pow(p1[0] - p2[0], 2) + math.pow(p1[1] - p2[1], 2))

class TreeNode(object):
    
    def distance(self, x, y):
        if self.geodesic:        
            start = ((self.x, 0, 0), (self.y, 0, 0))
            end = ((x, 0, 0), (y, 0, 0))
        
            return points2distance(start, end)
        else:
            return math.sqrt(math.pow(x - self.x,2) + math.pow(y - self.y, 2))
    
    def __init__(self, x, y, data, x_axis, geodesic):
        self.left = None
        self.right = None
        self.data = data
        self.x_axis = x_axis
        self.x = x
        self.y = y
        self.geodesic = geodesic
        
class TwoDTree(object):
    '''
    Simple 2d-tree
    '''
    
    def print_all(self):
        '''
        prints an inorder traversal of tree
        '''
        for node in self.to_list(self.root):
            print node.data
            
    def shuffle(self):
        '''
        shuffles the contents of the tree and reinserts them into a new tree
        '''
        
        list = self.to_list(self.root)
        random.shuffle(list)
        
        new_tree = TwoDTree()
        
        for node in list:
            new_tree.insert(node.x, node.y, node.data)
        
        return new_tree
    
    
    def fill(self, x_list, y_list, data_list):
        '''
        fills tree with perfect balance with the points listed in lists x, y, and data
        '''
        
        nodes = []
        
        # this is a bit hacky, but by constructing a list of TreeNodes, we can
        # use the balance method to implement this method 
        for x, y, data in zip(x_list, y_list, data_list):
            nodes.append(TreeNode(x, y, data, False, self.geodesic))        
            
        self.balance(nodes, self)
                
    
    def balance(self, list = "default", new_tree = None, x_axis = True):
        '''
        returns a perfectly balanced tree with this tree's data
        '''

        # set defaults
        if list == "default":
            list = self.to_list(self.root)
            new_tree = TwoDTree()
        
        # sort to find the median
        if x_axis:
            list = sorted(list, lambda a,b: cmp(a.x, b.x))
        else:
            list = sorted(list, lambda a,b: cmp(a.y, b.y))
        
        
        # remove the median from the list
        median = list.pop(len(list)/2)

        # insert the median into new_tree
        new_tree.insert(median.x, median.y, median.data) 
        
        # partition remaining nodes and recurse on children        
        left_nodes = []
        right_nodes = []
        for node in list:
            if x_axis and node.x <= median.x:
                left_nodes.append(node)
            elif x_axis and node.x > median.x:
                right_nodes.append(node)
            elif not x_axis and node.y <= median.y:
                left_nodes.append(node)
            elif not x_axis and node.y > median.y:
                right_nodes.append(node)
                
        if len(left_nodes) > 0:
            new_tree = self.balance(left_nodes, new_tree, not x_axis)
        if len(right_nodes) > 0:
            new_tree = self.balance(right_nodes, new_tree, not x_axis)
        
        return new_tree 
        
        
    def get_height(self, current = -1, h = 0):
        '''
        returns the maximum depth of the tree
        '''
        if self.height is -1:
            if current == -1:
                current = self.root
            elif current is None:
                return h
            return max(self.get_height(current.left, h+1), self.get_height(current.right, h+1))
        else:
            return self.height
        
    def get_xy(self):
        x = []
        y = []
        for node in self.to_list(self.root):
            x.append(node.x)
            y.append(node.y)
            
        return x, y

        
    def to_list(self, current):
        list = []
        if current:
            if current.left:
                list.extend(self.to_list(current.left))
            list.append(current)
            if current.right:
                list.extend(self.to_list(current.right))
        return list

    def insert(self, x, y, object):
        '''
        insert object into tree at (x, y)
        '''

        self.size += 1
        self.height = -1 # reset height (recompute next time height is called)
        
        if self.root:
            current = self.root
            inserted = False
            while not inserted:
                if current.x_axis:
                    if x <= current.x:
                        if current.left is None:
                            current.left = TreeNode(x, y, object, False, self.geodesic)
                            inserted = True
                        else:
                            current = current.left
                    else:
                        if current.right is None:
                            current.right = TreeNode(x, y, object, False, self.geodesic)
                            inserted = True
                        else:
                            current = current.right
                else:
                    if y <= current.y:
                        if current.left is None:
                            current.left = TreeNode(x, y, object, True, self.geodesic)
                            inserted = True
                        else:
                            current = current.left
                    else:
                        if current.right is None:
                            current.right = TreeNode(x, y, object, True, self.geodesic)
                            inserted = True
                        else:
                            current = current.right
        else:
            self.root = TreeNode(x, y, object, True, self.geodesic)
            
    def get(self, x, y):
        '''
        get object exactly at (x, y)
        '''

        current = self.root
        while current:
            if x == current.x and y == current.y:
                return current.data
            if current.x_axis:
                if x < current.x:
                    current = current.left
                else:
                    current = current.right
            else:
                if y < current.y:
                    current = current.left
                else:
                    current = current.right

        return None
    
    def knn(self, x, y, k = 1):
        '''
        get k nearest neighbors to (x, y)
        returns a tuple containing the list of k data entries, 
        the list of k x values and k y values
        can include node_set param, which excludes certain points
        '''

        nodes = []

        self.knn_helper(x, y, self.root, nodes, k)
            
        
        data = []
        x = []
        y = []
        
        for (_, node) in nodes:
            data.append(node.data)
            x.append(node.x)
            y.append(node.y)
            
        return data, (x, y)

    def knn_helper(self, x, y, current, nodes, k):
        '''
        partially based on pseudocode from wikipedia entry on kd-trees
        excludes any nodes in node_set
        invariant: nodes is always sorted by distance to (x, y) and of length <= k
        '''
        if current is None:
            return
        
        curr_dist = current.distance(x, y)
                
        if len(nodes) == 0 or curr_dist < nodes[-1][0]:
            nodes.append((curr_dist, current))
            nodes.sort()
            
            if len(nodes) > k:
                # list should be exactly k+1 long
                nodes.pop()
        
        if current.x_axis:
            if x <= current.x:
                child = current.left
            else:
                child = current.right
        else:
            if y <= current.y:
                child = current.left
            else:
                child = current.right
        self.knn_helper(x, y, child, nodes, k)
        
        best_dist = nodes[-1][0]
                
        if len(nodes) < k or (current.x_axis and current.distance(x, current.y) < best_dist) \
            or (not current.x_axis and current.distance(current.x, y) < best_dist):
            if current.x_axis:
                if x <= current.x:
                    child = current.right
                else:
                    child = current.left
            else:
                if y <= current.y:
                    child = current.right
                else:
                    child = current.left

            self.knn_helper(x, y, child, nodes, k)
              
    def within_radius(self, x, y, r):
        '''
        get all objects within radius r of (x, y)
        currently broken, need to rewrite 
        '''
        node_set = set([])
        
        distance = 0
        
        while distance < r:
            node = self.knn_helper(x, y, self.root, self.root, node_set)
            distance = node.distance(x, y)
            if distance < r:
                node_set.add(node)
            
        data = []
        x = []
        y = []
        for node in node_set:
            data.append(node.data)
            x.append(node.x)
            y.append(node.y)
        
        return data, (x, y)

    def set_geodesic(self, flag):
        self.geodesic = flag

    def __init__(self):
        '''
        Constructor
        '''
        
        self.root = None
        self.size = 0
        self.geodesic = False
        self.height = 0
        
if __name__ == "__main__":

    '''
    Test function
    '''
    
    import matplotlib.pyplot as plt
    
    tree = TwoDTree()
    tree.set_geodesic(True)
    
    for i in range(100):
        x = random.random()
        y = random.random()
        tree.insert(x, y, "%f, %f" % (x, y))
    
    tx = random.random()
    ty = random.random()
        
    (name, (nnx, nny)) = tree.knn(tx, ty, 10)
    
    (x,y) = tree.get_xy()
    
    plt.clf()
    plt.scatter(x, y, 1, 'b', 'o')
    plt.scatter(tx, ty, 30, 'b', '+')
    plt.scatter(nnx, nny, 50, 'b', 'o')
    
    from matplotlib.patches import Circle
    
    cir = Circle((tx, ty), distance((tx, ty), (nnx[-1], nny[-1])) , fc = 'none')
    plt.gca().add_patch(cir)
    
    plt.show()
    
#    # test radius
#    
#    r = 0.1
#    r = 10
#    
#    (name, (rx, ry)) = tree.within_radius(tx, ty, r)
#    
#    (x,y) = tree.get_xy()
#   
#    plt.figure()
#    plt.clf()
#    plt.scatter(x, y, 1, 'b', 'o')
#    plt.scatter(tx, ty, 30, 'b', '+')
#    plt.scatter(rx, ry, 50, 'b', 'o')
#    plt.plot((tx, tx), (ty - r, ty + r))
#    plt.plot((tx - r, tx + r), (ty, ty))
#    plt.show()
    
#    tree = TwoDTree()
#    
#    for i in range(4):
#        for j in range(3):
#            tree.insert(i + random.random(), j + random.random(), "(%d, %d)" % (i, j))
#    print "Tree size: %d. Tree height: %d" % (tree.size, tree.get_height())
#    
#    print tree.knn(30, 40, 10)
#    
#    tree = tree.shuffle()
#    print "Shuffled Tree size: %d. Tree height: %d" % (tree.size, tree.get_height())
#    
#    print tree.knn(30, 40, 10)
#    print tree.print_all()
#    
#    tree = tree.balance()
#    print "Balanced Tree size: %d. Tree height: %d" % (tree.size, tree.get_height())
#    
#    print tree.print_all()
    
    
    

    
