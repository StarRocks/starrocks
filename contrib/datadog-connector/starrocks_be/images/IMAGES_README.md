# Marketplace Media Carousel Guidelines

## Using the media gallery

To use the media gallery, you must upload a minimum of one image. The gallery
can hold a maximum of 8 pieces of media total, and one of these pieces of media
can be a video (guidelines and submission steps below). Images should be
added to your /images directory and referenced in the manifest.json file.


## Image and video requirements

### Images

```
File type       : .jpg or .png
File size       : ~500 KB per image, with a max of 1 MB per image
File dimensions : The aspect ratio must be 16:9 minimum, with these constraints:

                    Width: 1440px
                    Min height: 810px
                    Max height: 2560px

File name       : Use only letters, numbers, underscores, and hyphens
Color mode      : RGB
Color profile   : sRGB
Description     : 300 characters maximum
```

### Video

To display a video in your media gallery, please send our team the zipped file
or a link to download the video at `marketplace@datadog.com`. In addition,
please upload a thumbnail image for your video as a part of the pull request.
Once approved, we will upload the file to Vimeo and provide you with the
vimeo_id to add to your manifest.json file.  Please note that the gallery can
only hold one video.

```
File type       : MP4 H.264
File size       : Max 1 video; 1 GB maximum size
File dimensions : The aspect ratio must be exactly 16:9, and the resolution must be 1920x1080 or higher
File name       : partnerName-appName.mp4
Run time        : Recommendation of 60 seconds or less
Description     : 300 characters maximum
```
