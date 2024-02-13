/*
 *
 * Copyright (c) Sari Sultan (sarisultan@ieee.org | sari.sultan@mail.utoronto.ca)
 *
 * Part of the artifact evaluation code for Sultan et al.'s EuroSys'24 paper titled:
 * TTLs Matter: Efficient Cache Sizing with TTL-Aware Miss Ratio Curves and Working Set Sizes
 *
 * If you have any questions or want to report a bug please feel free to contact me anytime : )
 * If you want to optimize the code, please make a pull request and I will make sure to check it out, thanks.
 *
 */

using System.Runtime.CompilerServices;

namespace TTLsMatter.MRC.Olken;

public class OlkenAvl
{
    /// <summary>
    /// To relax memory pressure. I doubt we need more than one object.
    /// </summary>
    private Stack<AvlNode> _recycle = new();

    private const int _recycleSize = 10;

    public class AvlNode
    {
        public AvlNode(ulong keyhash, long sequenceNumber)
        {
            KeyHash = keyhash;
            SequenceNumber = sequenceNumber;
        }

        public void Reset()
        {
            Left = null;
            Right = null;
            BalanceFactor = 0;
            Height = 0;
            ChildrenCount = 0;

            SequenceNumber = 0;
            KeyHash = 0;
        }

        public AvlNode Left;
        public AvlNode Right;
        public sbyte BalanceFactor; //balance factor
        public sbyte Height; //the height of the node

        /// <summary>
        /// This could be the timestmap. Its better to make it long to avoid overflowing.
        /// This will be used to order the tree
        /// </summary>
        public long SequenceNumber;

        /// <summary>
        /// The hash of the request key
        /// </summary>
        public ulong KeyHash;


        /// <summary>
        /// This is the stack distance in number of distanct elements.
        /// </summary>
        public uint ChildrenCount;
    }

    /// <summary>
    /// Last operation children count.
    ///
    /// 0 means not found
    /// </summary>
    public long LastChildrenCount = 0;


    /// <summary>
    /// The number of nodes in the tree
    /// </summary>
    public long NodeCount;


    // /// <summary>
    // /// Height of the tree from the root
    // /// </summary>
    // public int Height => (Root == null) ? 0 : Root.Height;

    /// <summary>
    /// The root of the tree
    /// </summary>
    public AvlNode Root;

    #region methods

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetStackDistance(long timestamp, ulong keyhash)
    {
        LastChildrenCount = 0;
        var node = Root;
        while (node != null)
        {
            if (timestamp < node.SequenceNumber)
            {
                if (node.Right != null)
                {
                    LastChildrenCount += node.Right.ChildrenCount;
                    LastChildrenCount++;
                }

                LastChildrenCount++;
                node = node.Left;
            }
            else if (timestamp > node.SequenceNumber) node = node.Right;
            else
            {
                if (node.KeyHash != keyhash)
                    throw new Exception("AVL: Impossible case: keyhash should be equal");

                if (node.Right != null)
                {
                    LastChildrenCount += node.Right.ChildrenCount;

                    //we should add the right node because children sizes does not cover the node itself
                    LastChildrenCount++;
                }

                LastChildrenCount++;

                break;
            }
        }
    }

    public void Insert(ulong keyhash, long timestamp, uint evictionTime, uint sizeBytes)
    {
        //Assume this step is checked from the hashtable outside 
        if (_recycle.Any())
        {
            var node = _recycle.Pop();
            node.Reset();
            node.KeyHash = keyhash;
            node.SequenceNumber = timestamp;
            Root = Insert(Root, node);
        }
        else
        {
            Root = Insert(Root, new AvlNode(keyhash, timestamp));
        }

        NodeCount++;
    }

    private AvlNode Insert(AvlNode root, AvlNode node)
    {
        if (root == null) return node;

        if (node.SequenceNumber < root.SequenceNumber)
            root.Left = Insert(root.Left, node);
        if (node.SequenceNumber > root.SequenceNumber)
            root.Right = Insert(root.Right, node);

        //update height and balancer factors
        Update(root);

        //balance tree
        return Balance(root);
    }

    /// <summary>
    /// update the node heights and balancer factors
    /// </summary>
    /// <param name="node"></param>
    private void Update(AvlNode node)
    {
        sbyte leftNodeHeight = (node.Left == null) ? (sbyte)-1 : node.Left.Height;
        sbyte rightNodeHeight = (node.Right == null) ? (sbyte)-1 : node.Right.Height;

        node.Height = leftNodeHeight > rightNodeHeight ? leftNodeHeight : rightNodeHeight;
        node.Height++;

        node.BalanceFactor = (sbyte)(rightNodeHeight - leftNodeHeight);

        node.ChildrenCount = 0;
        if (node.Right != null)
        {
            node.ChildrenCount = node.Right.ChildrenCount + 1;
        }

        if (node.Left != null)
        {
            node.ChildrenCount += node.Left.ChildrenCount + 1;
        }
    }

    /// <summary>
    /// Balancing should occur when the node bf is +2 or -2
    /// </summary>
    /// <param name="node"></param>
    /// <returns></returns>
    private AvlNode Balance(AvlNode node)
    {
        //LEFT HEAVY CASE
        if (node.BalanceFactor == -2)
        {
            //left-left
            if (node.Left.BalanceFactor <= 0)
            {
                return LeftLeftCase(node);
            }
            else
            {
                return LeftRightCase(node);
            }
        }
        else if (node.BalanceFactor == 2)
        {
            //right right
            if (node.Right.BalanceFactor >= 0)
            {
                return RightRightCase(node);
            }
            else
            {
                return RightLeftCase(node);
            }
        }

        if (node.BalanceFactor != -1 && node.BalanceFactor != 0 && node.BalanceFactor != 1)
        {
            throw new Exception(
                $"Node balance factor error. At this point the bf should be in {{-1,0,1}} while it is {node.BalanceFactor}");
        }

        return node;
    }

    public void Remove(long timestamp)
    {
        //assume it contains the element from the hashtable
        Root = Remove(Root, timestamp);
        NodeCount--;
    }

    public AvlNode Remove(AvlNode root, long timestamp)
    {
        if (root == null) return null;

        if (timestamp < root.SequenceNumber)
        {
            root.Left = Remove(root.Left, timestamp);
        }
        else if (timestamp > root.SequenceNumber)
        {
            root.Right = Remove(root.Right, timestamp);
        }
        else
        {
            if (root.Left == null)
            {
                return root.Right;
            }
            else if (root.Right == null)
            {
                return root.Left;
            }
            else
            {
                if (root.Left.Height > root.Right.Height)
                {
                    AvlNode successorNode = FindMax(root.Left);
                    root.SequenceNumber = successorNode.SequenceNumber;
                    root.KeyHash = successorNode.KeyHash;
                    root.Left = Remove(root.Left, successorNode.SequenceNumber);

                    if (_recycle.Count < _recycleSize)
                    {
                        _recycle.Push(successorNode);
                    }
                }
                else
                {
                    AvlNode successorNode = FindMin(root.Right);
                    root.SequenceNumber = successorNode.SequenceNumber;
                    root.KeyHash = successorNode.KeyHash;
                    root.Right = Remove(root.Right, successorNode.SequenceNumber);

                    if (_recycle.Count < _recycleSize)
                    {
                        _recycle.Push(successorNode);
                    }
                }
            }
        }

        Update(root);
        return Balance(root);
    }

    public ulong RemoveLruItem()
    {
        var node = FindMin(Root);
        if (node != null)
        {
            Remove(node.SequenceNumber);
            return node.KeyHash;
        }
        else
        {
            return 0;
        }
    }

    private AvlNode FindMin(AvlNode node)
    {
        while (node.Left != null) node = node.Left;
        return node;
    }

    private AvlNode FindMax(AvlNode node)
    {
        while (node.Right != null) node = node.Right;
        return node;
    }

    #region CASES and rotations

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AvlNode LeftLeftCase(AvlNode node)
    {
        return RightRotation(node);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AvlNode LeftRightCase(AvlNode node)
    {
        node.Left = LeftRotation(node.Left);
        return LeftLeftCase(node);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AvlNode RightRightCase(AvlNode node)
    {
        return LeftRotation(node);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AvlNode RightLeftCase(AvlNode node)
    {
        node.Right = RightRotation(node.Right);
        return RightRightCase(node);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AvlNode RightRotation(AvlNode node)
    {
        var np = node.Left;
        node.Left = np.Right;
        np.Right = node;
        Update(node);
        Update(np);
        return np;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AvlNode LeftRotation(AvlNode node)
    {
        var np = node.Right;
        node.Right = np.Left;
        np.Left = node;
        Update(node);
        Update(np);
        return np;
    }

    #endregion

    #endregion

}