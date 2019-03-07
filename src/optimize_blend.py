# This script is meant to be run from within blender
print("Started running optimize_blend.py")
import bpy
import os
import json
import datetime

try:
    BENDER_OVERRIDEFORMAT = os.environ['BENDER_OVERRIDEFORMAT']
except KeyError:
    BENDER_OVERRIDEFORMAT = "PNG"


def now():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

history = {}
history[now()] = "optimize_blend.py: Sucessfully started blender with optimize_blend.py"


try:
    # Get current Scene
    scene = bpy.context.scene
    history[now()] = "optimize_blend.py: Active scene.name=\'"+scene.name+"\'"
except:
    print("Error: Couldn't get bpy.context.scene")

# Only allow still image formats to avoid video encoding
allowed_formats = ["PNG", "BMP", "JPEG", "JPEG2000", "TARGA", "TARGA_RAW", "CINEON", "DPX", "OPEN_EXR_MULTILAYER", "OPEN_EXR", "HDR", "TIFF"]

try:
    # Check if Cycles is used
    renderer = bpy.context.scene.render.engine
    uses_cycles = renderer == 'CYCLES'
    history[now()] = "optimize_blend.py: active renderer is "+renderer
except:
    print("Error: couldn't get bpy.context.scene.render.engine")

cuda = False

# Try to switch to GPU and CUDA if cycles is used
if uses_cycles:
    prefs = bpy.context.user_preferences.addons['cycles'].preferences
    if len(prefs.devices) > 0:
        history[now()] = "optimize_blend.py: Found these cycles devices: "+", ".join([str(d).replace("<bpy_struct, CyclesDeviceSettings(\"", "").replace("\")>", "") for d in prefs.devices])
        try:
            bpy.context.user_preferences.addon['cycles'].preferences.compute_device_type = 'CUDA'
            cuda = True
            history[now()] = "optimize_blend.py: Set compute_device_type to CUDA"
        except:
            history[now()] = "optimize_blend.py: Error: Failed to set compute_device_type to CUDA"
        if cuda:
            try:
                scene.cycles.device = 'GPU'
                history[now()] = "optimize_blend.py: Set scene.cycles.device to GPU"
            except:
                history[now()] = "optimize_blend.py: Error: Failed to set scene.cycles.device to GPU"
            scene.render.tile_x = 512
            scene.render.tile_y = 512
            history[now()] = "optimize_blend.py: Set scene render tiles to 512x512"
    else:
        history[now()] = "optimize_blend.py: No CUDA Devices found"

# If the value is not in the list of valid formats, override it
image_format = scene.render.image_settings.file_format
valid_format = image_format in allowed_formats

if not valid_format:
    scene.render.image_settings.file_format = BENDER_OVERRIDEFORMAT
    if scene.render.image_settings.file_format == "PNG":
        scene.render.image_settings.color_depth == "16"
    history[now()] = "optimize_blend.py: Output file format in file ("+str(image_format)+") was not in the list of valid formats. Used "+BENDER_OVERRIDEFORMAT+" instead!"

# Delete unused Materials:
n_materials = len(bpy.data.materials)
n_materials_removed = 0
for material in bpy.data.materials:
    if not material.users:
        bpy.data.materials.remove(material)
        n_materials_removed -= 1
if n_materials_removed > 0: history[now()] = "optimize_blend.py: Removed "+str(n_materials_removed)+" unused Materials"

# Delete unused Objects:
n_objects = len(bpy.data.objects)
n_objects_removed = 0
for obj in bpy.data.objects:
    if not obj.users:
        bpy.data.objects.remove(obj)
        n_objects_removed -= 1
if n_objects_removed > 0: history[now()] = "optimize_blend.py: Removed "+str(n_objects_removed)+" unused Objects"

# Delete unused Textures:
n_textures = len(bpy.data.textures)
n_textures_removed = 0
for texture in bpy.data.textures:
    if not texture.users:
        bpy.data.textures.remove(texture)
        n_textures_removed -= 1
if n_textures_removed > 0: history[now()] = "optimize_blend.py: Removed "+str(n_textures_removed)+" unused Textures"



# Overwrite the file
bpy.ops.wm.save_as_mainfile(filepath=bpy.data.filepath, copy=True)
history[now()] = "optimize_blend.py: Stored changes in file at "+bpy.data.filepath



# Save Status into dict
status = {
    "valid_format": valid_format,
    "path": bpy.data.filepath,
    "render":{
        "renderer": renderer,
        "cuda": cuda,
        "device": scene.cycles.device,
        "image_format": image_format,
        "uses_compositing": scene.render.use_compositing,
    },
    "materials": {
        "n": n_materials,
        "removed": n_materials_removed
    },
    "objects": {
        "n": n_objects,
        "removed": n_objects_removed
    },
    "textures": {
        "n": n_textures,
        "removed": n_textures_removed
    },
    "frames": {
        "start": scene.frame_start,
        "end": scene.frame_end,
        "current": scene.frame_current,
        "step": scene.frame_step,
        "fps": scene.render.fps
    },
    "resolution": {
        "x": scene.render.resolution_x,
        "y": scene.render.resolution_y,
        "scale": scene.render.resolution_percentage
    },
    "history": history
}

# Serialize to json
print(json.dumps(status))