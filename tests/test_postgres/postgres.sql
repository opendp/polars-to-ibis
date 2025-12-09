CREATE EXTENSION plpython3u;

CREATE FUNCTION hello(name text)
  RETURNS text
AS $$
  return f"Hello {name}!"
$$ LANGUAGE plpython3u;

SELECT hello("World");
