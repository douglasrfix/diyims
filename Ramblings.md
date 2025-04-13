This will be informal ramblings about the project until they,
 the ramblings, are moved to a better home.

Consider these as captured stream of conscious musings.

1. What is a news service anyway? At the moment, it is the publisher role
. This implies transmission of content, not necessarily creation of content
. This may be packaging of content from creators and advertisers.  It would
 mean revenue creation via subscribers and/or advertisers. It would mean
  paying the content providers.  Dealing with syndicators.

2. This started out as a project to replace the one man local newspaper. It
  needs an text editor, non text media manager. Storage space and
   distribution capability to a set of subscribers. It would be nice to be
    able to generate voice from the text as well.

3. How to handle security. How do I know who sent/created the content?

4. Resource utilization.
- Assume that the process will omly take notice of pubsub messages exchanged between nodes. It will only request data from the network upon request. Only the consumer can cause transmission of data. There is no platform that must be managed and requested permission of. No local strage is consumed by content unless the user wishes to see/hear/see it.
-
5. Freedom of speech.
- In a freedom of speech context, You can speak all you want to but you can't make anyone listen. The trick will be to sort out the wheat from the chaff. As a first cut I would imagine that a content flag will identify at a minimum porn or not porn. No flag the application will not retrieve it. Thia won't stop it from being published but the user can avoid the content. This would apply as well to unlawful contantas well. A provider of such content would be put on a black list and the provider would be banned from the application and the would not have the contnet propagted. I envision that a credability score might be developed for each provider.

6. This is not intended to satisfy the streaming/eye candy crowd.
- By it's nature, a request must initiate the process and your network may not provide sufficient resources to stisfy streaming. You would normaly "downlaod" before viewing the content.
- I would start with a summary description that would provide simple topic and summary to dtermine if the content appears interestinh. No video grabs or other animated foolishness, This could be fetched automatically by user preference for subscribed channels/topics/? etc.

7. This is a distributed application and therefore alive somewhere, hopefully at all times. This allows a "monolithic" architecture that provides an api that coexists with prior versions until they are retired. This allows parallel development and rollout with an asynchronous provision to willing users when it is convenient for them.


8. The network traffic consists primarily of transactions that are then applied to the local storage by the application. This constitutes the first api for the application.


4.2.1. Known issues
4.2.1.1. Redirection of local data, registry, and temporary paths
Because of restrictions on Microsoft Store apps, Python scripts may not have full write access to shared locations such as TEMP and the registry. Instead, it will write to a private copy. If your scripts must modify the shared locations, you will need to install the full installer.

At runtime, Python will use a private copy of well-known Windows folders and the registry. For example, if the environment variable %APPDATA% is c:\Users\<user>\AppData\, then when writing to C:\Users\<user>\AppData\Local will write to C:\Users\<user>\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.8_qbz5n2kfra8p0\LocalCache\Local\.

When reading files, Windows will return the file from the private folder, or if that does not exist, the real Windows directory. For example reading C:\Windows\System32 returns the contents of C:\Windows\System32 plus the contents of C:\Program Files\WindowsApps\package_name\VFS\SystemX86.

You can find the real path of any existing file using os.path.realpath():

>>>
import os
test_file = 'C:\\Users\\example\\AppData\\Local\\test.txt'
os.path.realpath(test_file)
'C:\\Users\\example\\AppData\\Local\\Packages\\PythonSoftwareFoundation.Python.3.8_qbz5n2kfra8p0\\LocalCache\\Local\\test.txt'
When writing to the Windows Registry, the following behaviors exist:

Reading from HKLM\\Software is allowed and results are merged with the registry.dat file in the package.

Writing to HKLM\\Software is not allowed if the corresponding key/value exists, i.e. modifying existing keys.

Writing to HKLM\\Software is allowed as long as a corresponding key/value does not exist in the package and the user has the correct access permissions.

For more detail on the technical basis for these limitations, please consult Microsoft’s documentation on packaged full-trust apps, currently available at docs.microsoft.com/en-us/windows/msix/desktop/desktop-to-uwp-behind-the-scenes
