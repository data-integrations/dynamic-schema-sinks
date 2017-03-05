# Dynamic Schema HBase Sink

This plugin support writing dynamic schemas to HBase in addition to write regular record to HBase. This plugin can
be used when you don't know how many columns need to written to HBase.

## Use-case

## Usage Notes

### Defining Dynamic Schema

### Row Key Generation

#### Types

#### Expression

Input record
```
  {
    "col1" : "name",
    "col2" : 2.8f,
    "col3" : 1,
    "other" : "other"
  }
```

```
  col1 + ":" + col2 + ":" + col3
```

Would generate row key as

```
  name:2.8:1
```

#### Constants
"c1"

## Configuration

## Additional Notes


