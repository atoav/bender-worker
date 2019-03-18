# This script is meant to be run from within blender
print("Started running optimize.py")
import bpy


try:
    # Get current Scene
    scene = bpy.context.scene
except:
    print("Error: Couldn't get bpy.context.scene")



try:
    # Check if Cycles is used
    renderer = bpy.context.scene.render.engine
    uses_cycles = renderer == 'CYCLES'
except:
    print("Error: couldn't get bpy.context.scene.render.engine")

cuda = False

# Try to switch to GPU and CUDA if cycles is used
if uses_cycles:
    prefs = bpy.context.user_preferences.addons['cycles'].preferences
    if len(prefs.devices) > 0:
        try:
            bpy.context.user_preferences.addon['cycles'].preferences.compute_device_type = 'CUDA'
            cuda = True
        except:
        if cuda:
            try:
                scene.cycles.device = 'GPU'
            except:
            scene.render.tile_x = 512
            scene.render.tile_y = 512



# Overwrite the file
bpy.ops.wm.save_as_mainfile(filepath=bpy.data.filepath, copy=True)
