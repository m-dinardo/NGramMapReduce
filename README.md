
# N-Gram Frequencies

This is a MapReduce job that, given an input text file or directory, will 
tabulate the frequencies of all N-Grams that exist in the corpus. N is 5
by default, and the setting is controlled by the GenericOptionsParser argument
"n".

# Example usage

Given an input file that contains the text "to be or not to be"

```
$ hadoop jar NGramMapReduce.jar NGramDriver -D n=3 /input/path /output/path
```

Output file will be:

```
be  2
be or 1
be or not 1
not to  1
not to be 1
or  1
or not  1
or not to 1
to  2
to be 2
to be or  1
```

# High N

Greater N produces proportionally greater output data relative to input
data. To avoid unintentional running of jobs that produce excess data
the N is restricted by default to N = 9. This can be overriden by the
GenercOptionsParser argument override=true.
