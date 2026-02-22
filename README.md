# Meta Ray-Ban Data Source

Data source for indexing local Meta Ray-Ban recordings exported from the Meta View app.

## What it does

- Scans a recordings folder for videos, photos, and audio files.
- Optionally auto-discovers likely Meta Ray-Ban folders when no path is provided.
- Returns structured metadata for each recording.
- Optionally loads sidecar JSON metadata files (same filename, `.json` extension).

## Connection data

`connection_type`: `META_RAYBAN`

Fields:

- `recordings_path` (optional)
- `recursive` (default: `true`)
- `max_items` (default: `0`, meaning all)
- `include_videos` (default: `true`)
- `include_photos` (default: `true`)
- `include_audio` (default: `true`)
- `include_sidecar_metadata` (default: `true`)
- `include_file_hash` (default: `false`)
- `connection_name` (optional)

## Example params

```json
{
  "recordings_path": "C:/Users/you/Downloads/Ray-Ban Meta",
  "recursive": true,
  "max_items": 200,
  "include_videos": true,
  "include_photos": true,
  "include_audio": true,
  "include_sidecar_metadata": true,
  "include_file_hash": false
}
```
